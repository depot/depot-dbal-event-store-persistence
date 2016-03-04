<?php

namespace Depot\EventStore\Persistence\Adapter\Dbal;

use DateTimeImmutable;
use Depot\Contract\Contract;
use Depot\Contract\ContractResolver;
use Depot\EventStore\CommittedEventVisitor;
use Depot\EventStore\Management\Criteria;
use Depot\EventStore\Management\EventStoreManagement;
use Depot\EventStore\Persistence\CommittedEvent;
use Depot\EventStore\Persistence\OptimisticConcurrencyFailed;
use Depot\EventStore\Raw\RawCommittedEvent;
use Depot\EventStore\Raw\RawCommittedEventVisitor;
use Depot\EventStore\Raw\RawEventEnvelope;
use Depot\EventStore\Transaction\CommitId;
use Depot\EventStore\EventEnvelope;
use Depot\EventStore\Serialization\Serializer;
use Depot\EventStore\Persistence\Persistence;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Connection;

class DbalPersistence implements Persistence, EventStoreManagement
{
    /**
     * @var Connection
     */
    private $connection;

    /**
     * @var ContractResolver
     */
    private $eventContractResolver;

    /**
     * @var ContractResolver
     */
    private $metadataContractResolver;

    /**
     * @var Serializer
     */
    private $eventSerializer;

    /**
     * @var Serializer
     */
    private $metadataSerializer;

    /**
     * @var string
     */
    private $tableName;

    /**
     * @param Connection $connection
     * @param Serializer $eventSerializer
     * @param Serializer $metadataSerializer
     * @param ContractResolver$eventContractResolver
     * @param ContractResolver$metadataContractResolver
     * @param string $tableName
     */
    public function __construct(
        Connection $connection,
        Serializer $eventSerializer,
        Serializer $metadataSerializer,
        ContractResolver $eventContractResolver,
        ContractResolver $metadataContractResolver,
        $tableName = 'event'
    ) {
        $this->connection = $connection;
        $this->tableName = $tableName;
        $this->eventSerializer = $eventSerializer;
        $this->metadataSerializer = $metadataSerializer;
        $this->eventContractResolver = $eventContractResolver;
        $this->metadataContractResolver = $metadataContractResolver;
    }

    /**
     * @param Schema $schema
     *
     * @return \Doctrine\DBAL\Schema\Table
     */
    public function configureSchema(Schema $schema)
    {
        if ($schema->hasTable($this->tableName)) {
            return null;
        }

        return $this->configureTable();
    }

    private function configureTable()
    {
        $uuidType = 'string';
        $uuidParams = [
            'length' => 32,
            'limit'  => 32,
        ];

        $stringParams = [
            'length' => 128,
            'limit'  => 128,
        ];

        $schema = new Schema();
        $table = $schema->createTable($this->tableName);
        $table->addColumn('committed_event_id', 'integer', ["unsigned" => true, "autoincrement" => true]);
        $table->addColumn('commit_id', $uuidType, $uuidParams);
        $table->addColumn('utc_committed_time', 'datetime');
        $table->addColumn('aggregate_root_type', 'string', $stringParams);
        $table->addColumn('aggregate_root_id', $uuidType, $uuidParams);
        $table->addColumn('aggregate_root_version', 'integer');
        $table->addColumn('event_type', 'string', $stringParams);
        $table->addColumn('event_id', $uuidType, $uuidParams);
        $table->addColumn('event', 'text');
        $table->addColumn('event_version', 'integer');
        $table->addColumn('when', 'datetime');
        $table->addColumn('metadata_type', 'string', array_merge($stringParams, ['notnull' => false]));
        $table->addColumn('metadata', 'text', ['notnull' => false]);
        $table->setPrimaryKey(['committed_event_id']);
        $table->addIndex(['aggregate_root_type', 'aggregate_root_id', 'aggregate_root_version']);
        $table->addUniqueIndex(['aggregate_root_type', 'aggregate_root_id', 'event_version']);

        return $table;
    }

    public function fetch(Contract $aggregateRootType, $aggregateRootId)
    {
        $eventEnvelopes = [];

        $result = $this->findByaggregateRootTypeAndId($aggregateRootType, $aggregateRootId);

        while ($record = $result->fetch()) {
            $event = json_decode($record['event'], true);
            $metadata = $record['metadata_type']
                ? json_decode($record['metadata'], true)
                : null
            ;

            $eventType = $this->eventContractResolver->resolveFromContractName($record['event_type']);
            $metadataType = $record['metadata_type']
                ? $this->metadataContractResolver->resolveFromContractName($record['metadata_type'])
                : null
            ;

            $metadata = $metadata
                ? $this->metadataSerializer->deserialize($metadataType, $metadata)
                : null
            ;

            $eventEnvelopes[] = new EventEnvelope(
                $eventType,
                $record['event_id'],
                $this->eventSerializer->deserialize($eventType, $event),
                (int) $record['event_version'],
                new DateTimeImmutable($record['when']),
                $metadataType,
                $metadata
            );
        }

        return $eventEnvelopes;
    }

    /**
     * @param CommitId $commitId
     * @param Contract $aggregateRootType
     * @param string $aggregateRootId
     * @param int $expectedAggregateRootVersion
     * @param EventEnvelope[] $eventEnvelopes
     * @param DateTimeImmutable|null $now
     */
    public function commit(
        CommitId $commitId,
        Contract $aggregateRootType,
        $aggregateRootId,
        $expectedAggregateRootVersion,
        array $eventEnvelopes,
        $now = null
    ) {
        $aggregateRootVersion = $this->versionFor($aggregateRootType, $aggregateRootId);

        if ($aggregateRootVersion !== $expectedAggregateRootVersion) {
            throw new OptimisticConcurrencyFailed(
                $aggregateRootType->getContractName(),
                $aggregateRootId,
                sprintf(
                    'Expected aggregate root version %d but found %d.',
                    $expectedAggregateRootVersion,
                    $aggregateRootVersion
                )
            );
        }

        if (! $now) {
            $now = new \DateTimeImmutable('now');
        }

        $this->connection->beginTransaction();

        try {
            foreach ($eventEnvelopes as $eventEnvelope) {
                $metadata = $eventEnvelope->getMetadataType()
                    ? json_encode(
                        $this->metadataSerializer->serialize(
                            $eventEnvelope->getMetadataType(),
                            $eventEnvelope->getMetadata()
                        )
                    )
                    : null;
                $values = [
                    'commit_id' => $commitId,
                    'utc_committed_time' => $now->format('Y-m-d H:i:s'),
                    'aggregate_root_type' => $aggregateRootType->getContractName(),
                    'aggregate_root_id' => $aggregateRootId,
                    'aggregate_root_version' => $aggregateRootVersion,
                    'event_type' => $eventEnvelope->getEventType()->getContractName(),
                    'event_id' => $eventEnvelope->getEventId(),
                    'event' => json_encode(
                        $this->eventSerializer->serialize(
                            $eventEnvelope->getEventType(),
                            $eventEnvelope->getEvent()
                        )
                    ),
                    'event_version' => $eventEnvelope->getVersion(),
                    '`when`' => $eventEnvelope->getWhen()->format('Y-m-d H:i:s'),
                    'metadata_type' => $eventEnvelope->getMetadataType()
                        ? $eventEnvelope->getMetadataType()->getContractName()
                        : null,
                    'metadata' => $metadata,
                ];
                $this->connection->insert($this->tableName, $values);
            }

            $this->connection->commit();
        } catch (\Exception $e) {
            $this->connection->rollBack();

            if ($e instanceof \Doctrine\DBAL\DBALException) {
                if ($e->getPrevious() && in_array($e->getPrevious()->getCode(), [23000, 23050])) {
                    throw new OptimisticConcurrencyFailed(
                        $aggregateRootType->getContractName(),
                        $aggregateRootId,
                        'Duplicate event version.',
                        $e
                    );
                }
            }
        }
    }

    private function findByAggregateRootTypeAndId($aggregateRootType, $aggregateRootId)
    {
        $query = "SELECT * FROM "
            .$this->tableName.
            " WHERE aggregate_root_type = :aggregateRootType
            AND aggregate_root_id = :aggregateRootId ORDER BY aggregate_root_version";
        $statement = $this->connection->prepare($query);
        $statement->bindValue('aggregateRootType', $aggregateRootType->getContractName());
        $statement->bindValue('aggregateRootId', $aggregateRootId);
        $statement->execute();

        return $statement;
    }

    private function versionFor(Contract $aggregateRootType, $aggregateRootId)
    {
        $version = -1;

        $result = $this->findByaggregateRootTypeAndId($aggregateRootType, $aggregateRootId);

        while ($row = $result->fetch()) {
            if ($row['event_version'] > $version) {
                $version = $row['event_version'];
            }
        }

        return (int) $version;
    }

    private function deserializeCommittedEvent($row)
    {
        return new CommittedEvent(
            CommitId::fromString($row['commit_id']),
            DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['utc_committed_time']),
            new Contract($row['aggregate_root_type'], str_replace('.', '\\', $row['aggregate_root_type'])),
            $row['aggregate_root_id'],
            (int) $row['aggregate_root_version'],
            new EventEnvelope(
                $this->eventContractResolver->resolveFromContractName($row['event_type']),
                $row['event_id'],
                $this->eventSerializer->deserialize(
                    $this->eventContractResolver->resolveFromContractName($row['event_type']),
                    json_decode($row['event'], true)
                ),
                (int) $row['event_version'],
                DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['when']),
                $row['metadata_type'] ? $this->metadataContractResolver->resolveFromContractName($row['metadata_type']) : null,
                $row['metadata_type'] ? $this->eventSerializer->deserialize(
                    $this->metadataContractResolver->resolveFromContractName($row['metadata_type']),
                    json_decode($row['metadata'], true)
                ) : null
            )
        );
    }

    public function visitCommittedEvents(
        Criteria $criteria,
        CommittedEventVisitor $committedEventVisitor,
        RawCommittedEventVisitor $fallbackRawCommittedEventVisitor = null
    ) {
        $statement = $this->prepareVisitCommittedEventsStatement($criteria);
        $statement->execute();

        while ($row = $statement->fetch()) {
            $committedEvent = null;

            try {
                $committedEvent = $this->deserializeCommittedEvent($row);
            } catch (\Exception $e) {
                if (! $fallbackRawCommittedEventVisitor) {
                    throw $e;
                }

                $rawCommittedEvent = $this->deserializeRawCommittedEvent($row);

                $fallbackRawCommittedEventVisitor->doWithRawCommittedEvent($rawCommittedEvent);
            }

            if ($committedEvent) {
                $committedEventVisitor->doWithCommittedEvent($committedEvent);
            }
        }
    }

    private function deserializeRawCommittedEvent($row)
    {
        return new RawCommittedEvent(
            CommitId::fromString($row['commit_id']),
            DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['utc_committed_time']),
            new Contract($row['aggregate_root_type'], str_replace('.', '\\', $row['aggregate_root_type'])),
            $row['aggregate_root_id'],
            (int) $row['aggregate_root_version'],
            new RawEventEnvelope(
                $this->eventContractResolver->resolveFromContractName($row['event_type']),
                $row['event_id'],
                json_decode($row['event'], true),
                (int) $row['event_version'],
                DateTimeImmutable::createFromFormat('Y-m-d H:i:s', $row['when']),
                $row['metadata_type'] ? $this->metadataContractResolver->resolveFromContractName($row['metadata_type']) : null,
                $row['metadata_type'] ? json_decode($row['metadata'], true) : null
            )
        );
    }

    public function visitRawCommittedEvents(Criteria $criteria, RawCommittedEventVisitor $rawCommittedEventVisitor)
    {
        $statement = $this->prepareVisitCommittedEventsStatement($criteria);
        $statement->execute();

        while ($row = $statement->fetch()) {
            $rawCommittedEvent = $this->deserializeRawCommittedEvent($row);

            $rawCommittedEventVisitor->doWithRawCommittedEvent($rawCommittedEvent);
        }
    }


    private function prepareVisitCommittedEventsStatement(Criteria $criteria)
    {
        list ($where, $bindValues, $bindValueTypes) =
            $this->prepareVisitCommittedEventsStatementWhereAndBindValues($criteria);
        $query = 'SELECT *
            FROM ' . $this->tableName . '
            ' . $where . '
            ORDER BY committed_event_id ASC';

        $statement = $this->connection->executeQuery($query, $bindValues, $bindValueTypes);

        return $statement;
    }

    private function prepareVisitCommittedEventsStatementWhereAndBindValues(Criteria $criteria)
    {
        $bindValues = [];
        $bindValueTypes = [];

        $criteriaTypes = [];

        if ($criteria->getAggregateRootTypes()) {
            $criteriaTypes[] = 'aggregate_root_type IN (:aggregateRootTypes)';
            $aggregateRootTypeContractNames = array_map(function (Contract $aggregateRootType) {
                return $aggregateRootType->getContractName();
            }, $criteria->getAggregateRootTypes());
            $bindValues['aggregateRootTypes'] = $aggregateRootTypeContractNames;
            $bindValueTypes['aggregateRootTypes'] = Connection::PARAM_STR_ARRAY;
        }

        if ($criteria->getAggregateRootIds()) {
            $criteriaTypes[] = 'aggregate_root_id IN (:aggregateRootIds)';
            $bindValues['aggregateRootIds'] = $criteria->getAggregateRootIds();
            $bindValueTypes['aggregateRootIds'] = Connection::PARAM_STR_ARRAY;
        }

        if ($criteria->getEventTypes()) {
            $criteriaTypes[] = 'event_type IN (:eventTypes)';
            $eventTypeContractNames = array_map(function (Contract $eventType) {
                return $eventType->getContractName();
            }, $criteria->getEventTypes());
            $bindValues['eventTypes'] = $eventTypeContractNames;
            $bindValueTypes['eventTypes'] = Connection::PARAM_STR_ARRAY;
        }

        if (! $criteriaTypes) {
            return array('', [], []);
        }

        $where = 'WHERE '.join(' AND ', $criteriaTypes);

        return array($where, $bindValues, $bindValueTypes);
    }
}
