<?php

namespace Depot\EventStore\Persistence\Adapter\Dbal;

use DateTimeImmutable;
use Depot\Contract\Contract;
use Depot\Contract\ContractResolver;
use Depot\EventStore\Persistence\OptimisticConcurrencyFailed;
use Depot\EventStore\Transaction\CommitId;
use Depot\EventStore\EventEnvelope;
use Depot\EventStore\Serialization\Serializer;
use Depot\EventStore\Persistence\Persistence;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Connection;

class DbalPersistence implements Persistence
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
        $table->addColumn('aggregate_type', 'string', $stringParams);
        $table->addColumn('aggregate_id', $uuidType, $uuidParams);
        $table->addColumn('aggregate_version', 'integer');
        $table->addColumn('event_type', 'string', $stringParams);
        $table->addColumn('event_id', $uuidType, $uuidParams);
        $table->addColumn('event', 'text');
        $table->addColumn('when', 'datetime');
        $table->addColumn('metadata_type', 'string', array_merge($stringParams, ['notnull' => false]));
        $table->addColumn('metadata', 'text', ['notnull' => false]);
        $table->setPrimaryKey(['committed_event_id']);
        $table->addIndex(['aggregate_type', 'aggregate_id', 'aggregate_version']);

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
                $aggregateRootType,
                $aggregateRootId,
                $eventType,
                $record['event_id'],
                $this->eventSerializer->deserialize($eventType, $event),
                $record['aggregate_version'],
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
     */
    public function commit(
        CommitId $commitId,
        Contract $aggregateRootType,
        $aggregateRootId,
        $expectedAggregateRootVersion,
        array $eventEnvelopes
    ) {
        $aggregateRootVersion = $this->versionFor($aggregateRootType, $aggregateRootId);

        if ($aggregateRootVersion !== $expectedAggregateRootVersion) {
            throw new OptimisticConcurrencyFailed();
        }

        $utcCommittedTime = new \DateTimeImmutable('now');

        foreach ($eventEnvelopes as $eventEnvelope) {
            $metadata = $eventEnvelope->getMetadataType()
                ? json_encode($this->metadataSerializer->serialize($eventEnvelope->getMetadataType(), $eventEnvelope->getMetadata()))
                : null
            ;
            $values = [
                'commit_id' => $commitId,
                'utc_committed_time' => $utcCommittedTime->format('Y-m-d H:i:s'),
                'aggregate_type' => $aggregateRootType->getContractName(),
                'aggregate_id' => $aggregateRootId,
                'aggregate_version' => ++$aggregateRootVersion,
                'event_type' => $eventEnvelope->getEventType()->getContractName(),
                'event_id' => $eventEnvelope->getEventId(),
                'event' => json_encode($this->eventSerializer->serialize($eventEnvelope->getEventType(), $eventEnvelope->getEvent())),
                '`when`' => $eventEnvelope->getWhen()->format('Y-m-d H:i:s'),
                'metadata_type' => $eventEnvelope->getMetadataType()
                    ? $eventEnvelope->getMetadataType()->getContractName()
                    : null,
                'metadata' => $metadata,
            ];
            $this->connection->insert($this->tableName, $values);

        }
    }

    private function findByaggregateRootTypeAndId($aggregateRootType, $aggregateRootId)
    {
        $query = "SELECT * FROM ".$this->tableName." WHERE aggregate_type = :aggregateRootType AND aggregate_id = :aggregateRootId ORDER BY aggregate_version";
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
            if ($row['aggregate_version'] > $version) {
                $version = $row['aggregate_version'];
            }
        }

        return (int) $version;
    }
}
