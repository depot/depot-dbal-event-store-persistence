<?php

namespace Monii\AggregateEventStorage\EventStore\Persistence\Adapter\Dbal;

use Monii\AggregateEventStorage\EventStore\Persistence\Persistence;
use Doctrine\DBAL\Schema\Schema;
use Doctrine\DBAL\Connection;

class DbalPersistence
{
    /**
     * @param $connection
     * @param $tableName
     */
    public function __construct(Connection $connection, $tableName = 'event')
    {
        $this->connection = $connection;
        $this->tableName = $tableName;
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
        $table->setIndex(['aggregate_type', 'aggregate_id', 'aggregate_version'], 'aggregate');

        return $table;
    }

    /**
     * {@inheritdoc}
     */
    public function save(Persistence $persistence)
    {
        $values = array(
            'commit_id' => $persistence->commitId,
            'utc_committed_time' => $persistence->utcCommittedTime,
            'aggregate_type' => $persistence->aggregateType,
            'aggregate_id' => $persistence->aggregateId,
            'aggregate_version' => $persistence->aggregateVersion,
            'event_type' => $persistence->eventType,
            'event_id' => $persistence->eventId,
            'event' => $persistence->event,
            'metadata_type' => $persistence->metadataType,
            'metadata' => $persistence->metadata
        );
        $this->connection->insert($this->tableName, $values);
    }
}