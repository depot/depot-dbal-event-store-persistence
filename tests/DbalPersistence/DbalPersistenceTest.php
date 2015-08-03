<?php

namespace Monii\AggregateEventStorage\EventStore\Persistence\Adapter\Dbal;

use Doctrine\DBAL\Portability\Connection;
use Doctrine\DBAL\DriverManager;
use Monii\AggregateEventStorage\EventStore\Persistence\PersistenceTest;
use Monii\AggregateEventStorage\Contract\SimplePhpFqcnContractResolver;
use Monii\AggregateEventStorage\EventStore\Serialization\Adapter\PropertiesReflection\PropertiesReflectionSerializer;
use Monii\AggregateEventStorage\EventStore\Persistence\Adapter\Dbal\DbalPersistence;

class DbalPersistenceTest extends PersistenceTest
{
    private $connection;

    private $dbalPersistence;

    protected function createPersistence()
    {
        $serializer = new PropertiesReflectionSerializer(
            new SimplePhpFqcnContractResolver()
        );

        $this->connection = $this->getConnection();

        $schemaManager = $this->connection->getSchemaManager();
        $schema = $schemaManager->createSchema();

        //$dbalPersistence = $this->createPersistence();
        //

        $this->dbalPersistence = new DbalPersistence(
            $this->connection,
            'event',
            $serializer,
            $serializer
        );
        $this->dbalPersistence->configureSchema($schema);

        return $this->dbalPersistence;
    }

    protected function getPersistence()
    {
        return $this->dbalPersistence;
    }

    /**
     * @return Connection
     * @throws \Doctrine\DBAL\DBALException
     */
    private function getConnection()
    {
        if ($this->connection) {
            return $this->connection;
        }
        $this->connection = DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true
        ]);
        return $this->connection;
    }
}