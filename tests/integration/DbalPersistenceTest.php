<?php

namespace Depot\Testing\Integration\EventStore\Persistence\Adapter\Dbal;

use Doctrine\DBAL\Portability\Connection;
use Doctrine\DBAL\DriverManager;
use Depot\Testing\EventStore\Persistence\PersistenceTest;
use Depot\Contract\SimplePhpFqcnContractResolver;
use Depot\EventStore\Persistence\Adapter\Dbal\DbalPersistence;
use Depot\EventStore\Serialization\Adapter\MoniiReflectionPropertiesSerializer\MoniiReflectionPropertiesSerializer;

class DbalPersistenceTest extends PersistenceTest
{
    private $dbalPersistence;

    protected function createPersistence()
    {
        $serializer = new MoniiReflectionPropertiesSerializer(
            new SimplePhpFqcnContractResolver()
        );

        $contractResolver = new SimplePhpFqcnContractResolver();

        $connection = $this->getConnection();

        $schemaManager = $connection->getSchemaManager();
        $schema = $schemaManager->createSchema();

        $this->dbalPersistence = new DbalPersistence(
            $connection,
            $serializer,
            $serializer,
            $contractResolver,
            $contractResolver,
            'event'
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
