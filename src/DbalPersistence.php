<?php

namespace Monii\AggregateEventStorage\EventStore\Persistence\Adapter\Dbal;

use Monii\AggregateEventStorage\Contract\Contract;
use Monii\AggregateEventStorage\EventStore\Transaction\CommitId;
use Monii\AggregateEventStorage\EventStore\EventEnvelope;
use Monii\AggregateEventStorage\EventStore\Serialization\Serializer;
use Monii\AggregateEventStorage\EventStore\Persistence\Persistence;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Connection;

class DbalPersistence implements Persistence
{
    /**
     * @var Serializer
     */
    private $eventSerializer;

    /**
     * @var Serializer
     */
    private $metadataSerializer;

    /**
     * @param $connection
     * @param $tableName
     * @param $eventSerializer
     * @param $metadataSerializer
     */
    public function __construct(
        Connection $connection,
        $tableName = 'event',
        Serializer $eventSerializer,
        Serializer $metadataSerializer
    )
    {
        $this->connection = $connection;
        $this->tableName = $tableName;
        $this->eventSerializer = $eventSerializer;
        $this->metadataSerializer = $metadataSerializer;
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
        $table->addColumn('committed_event_id', 'int', ["unsigned" => true, "autoincrement" => true]);
        $table->addColumn('commit_id', $uuidType, $uuidParams);
        $table->addColumn('utc_committed_time', 'datetime');
        $table->addColumn('aggregate_type', 'string', $stringParams);
        $table->addColumn('aggregate_id', $uuidType, $uuidParams);
        $table->addColumn('aggregate_version', 'int');
        $table->addColumn('event_type', 'string', $stringParams);
        $table->addColumn('event_id', $uuidType, $uuidParams);
        $table->addColumn('event', 'text');
        $table->addColumn('metadata_type', 'string', $stringParams);
        $table->addColumn('metadata', 'text');
        $table->setPrimaryKey(['committed_event_id']);
        $table->setIndex(['aggregate_type', 'aggregate_id', 'aggregate_version']);

        return $table;
    }

    public function fetch(Contract $aggregateType = null, $aggregateId = null)
    {
        if (is_null($aggregateType) || is_null($aggregateId)) {
            return;
        }

        $eventEnvelopes = [];

        $row = $this->findByAggregateTypeAndId($aggregateType, $aggregateId);

        foreach ($row as $record) {

            $eventEnvelopes[] = new EventEnvelope(
                $record->eventType,
                $record->eventId,
                $this->eventSerializer->deserialize($record->eventType, $record->event),
                $record->metadataType,
                $record->metadataType
                    ? $this->metadataSerializer->deserialize($record->metadataType, $record->metadata)
                    : null
            );

        }

        return $eventEnvelopes;
    }

    /**
     * @param CommitId $commitId
     * @param Contract $aggregateType
     * @param string $aggregateId
     * @param int $expectedAggregateVersion
     * @param EventEnvelope[] $eventEnvelopes
     */
    public function commit(
        CommitId $commitId,
        Contract $aggregateType,
        $aggregateId,
        $expectedAggregateVersion,
        array $eventEnvelopes
    )
    {
        $aggregateVersion = $expectedAggregateVersion;

        // Todo: Add transaction / rollback on exception

        foreach ($eventEnvelopes as $eventEnvelope) {

            $values = array(
                'commit_id' => $commitId,
                'utc_committed_time' => new \DateTimeImmutable('now'),
                'aggregate_type' => $aggregateType,
                'aggregate_id' => $aggregateId,
                'aggregate_version' => ++$aggregateVersion,
                'event_type' => $eventEnvelope->getEventType(),
                'event_id' => $eventEnvelope->getEventId(),
                'event' => $this->eventSerializer->serialize($eventEnvelope->getEventType(), $eventEnvelope->getEvent()),
                'metadata_type' => $eventEnvelope->getMetadataType(),
                'metadata' => (
                $eventEnvelope->getMetadataType()
                    ? $this->metadataSerializer->serialize($eventEnvelope->getMetadataType(), $eventEnvelope->getMetadata())
                    : null
                )
            );
            $this->connection->insert($this->tableName, $values);

        }
    }

    private function findByAggregateTypeAndId($aggregateType, $aggregateId)
    {
        $query = 'SELECT * FROM '.$this->tableName.' WHERE aggregate_type = :aggregateType AND aggregate_id = :aggregateId ORDER BY utc_committed_time';
        $statement = $this->connection->prepare($query);
        $statement->bindValue(':aggregateType', $aggregateType);
        $statement->bindValue(':aggregateId', $aggregateId);
        $statement->execute();
        $result = $statement->fetch();
        return $result;
    }
}