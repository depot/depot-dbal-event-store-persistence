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
    private $dbalPersistence;

    protected function createPersistence()
    {
        $serializer = new PropertiesReflectionSerializer(
            new SimplePhpFqcnContractResolver()
        );

        $contractResolver = new SimplePhpFqcnContractResolver();

        $connection = $this->getConnection();

        $schemaManager = $connection->getSchemaManager();
        $schema = $schemaManager->createSchema();

        $this->dbalPersistence = new DbalPersistence(
            $connection,
            'event',
            $serializer,
            $serializer,
            $contractResolver,
            $contractResolver
        );
        $table = $this->dbalPersistence->configureSchema($schema);

        if ($table) {
            $schemaManager->createTable($table);
        }

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
        return  DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true
        ]);
    }
}