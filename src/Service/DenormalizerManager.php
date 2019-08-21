<?php

namespace Drupal\denormalizer\Service;

use Drupal\Core\Entity\EntityFieldManagerInterface;
use Drupal\Core\Entity\EntityTypeInterface;
use Drupal\Core\Entity\EntityTypeManagerInterface;
use Drupal\Core\Entity\ContentEntityType;
use Drupal\Core\Database\Connection;
use Drupal\denormalizer\Exception\DatabaseCreationNotSupportedException;

/**
 * Class DenormalizerManager
 *
 * @package Drupal\denormalizer\Service
 */
class DenormalizerManager implements DenormalizerManagerInterface {

  /**
   * The entity type service.
   *
   * @var EntityTypeManagerInterface
   */
  protected $entityTypeManager;

  /**
   * The entity field manager service.
   *
   * @var EntityFieldManagerInterface
   */
  protected $entityFieldManager;

  /**
   * The database connection service.
   *
   * @var Connection
   */
  protected $connection;

  public function __construct(EntityTypeManagerInterface $entityTypeManager, EntityFieldManagerInterface $entityFieldManager, Connection $connection) {
    $this->entityTypeManager = $entityTypeManager;
    $this->entityFieldManager = $entityFieldManager;
    $this->connection = $connection;
  }

  /**
   * {@inheritdoc}
   */
  public function getContentEntityTypes() {
    $contentEntityTypes = [];
    $entityTypeDefinitions = $this->entityTypeManager->getDefinitions();

    /* @var $definition EntityTypeInterface */
    foreach ($entityTypeDefinitions as $definition) {
      if ($definition instanceof ContentEntityType) {
        $contentEntityTypes[] = $definition->id();
      }
    }

    return $contentEntityTypes;
  }

  /**
   * {@inheritdoc}
   */
  public function getContentEntityFieldSchema(string $contentEntityMachineName, string $bundle = NULL) {
    $fields = array();
    if ($contentEntityMachineName == 'node' || $contentEntityMachineName == 'taxonomy_term') {
      if (!isset($bundle)) {
        throw new \Exception("Missing bundle for $contentEntityMachineName.");
      }

      $fieldDefinitions = $this->entityFieldManager->getFieldDefinitions($contentEntityMachineName, $bundle);

      foreach ($fieldDefinitions as $fieldDefinition) {
        $schema = $fieldDefinition->getFieldStorageDefinition()->getSchema()['columns'];
        $fields[] = [
          'name' => $fieldDefinition->getName(),
          //'type' => $fieldDefinition->getType(),
          'schema' => $fieldDefinition->getType() == 'entity_reference' ? $schema['target_id'] : $schema['value']
        ];
      }
      return $fields;
    }

    $fieldStorageDefinitions = $this->entityFieldManager->getFieldStorageDefinitions($contentEntityMachineName);

    foreach ($fieldStorageDefinitions as $fieldStorageDefinition) {
      $schema = $fieldStorageDefinition->getSchema()['columns'];
      $fields[] = [
        'name' => $fieldStorageDefinition->getName(),
        'schema' => $fieldStorageDefinition->getType() == 'entity_reference' ? $schema['target_id'] : $schema['value']
      ];
    }

    return $fields;
  }

  /**
   * {@inheritdoc}
   */
  public function createDenormalizedTable(string $tableName, array $fieldDefinitions, string $key = 'default') {
    \Drupal\Core\Database\Database::setActiveConnection($key);

    $database = \Drupal\Core\Database\Database::getConnection();
    $schema[$tableName] = array(
      'fields' => array()
    );

    foreach ($fieldDefinitions as $fieldDefinition) {
      if (isset($fieldDefinition['schema'])) {
        $schema[$tableName]['fields'][$fieldDefinition['name']] = $fieldDefinition['schema'];
      }
    }

    $database->schema()->createTable($tableName, $schema[$tableName]);
    \Drupal\Core\Database\Database::setActiveConnection();
  }

  /**
   * {@inheritdoc}
   */
  public function createDatabase(string $key) {
    if ($key == 'default') {
      throw new DatabaseCreationNotSupportedException("The database associated with the default key already exists");
    }
    //\Drupal\Core\Database\Database::setActiveConnection($key);
    //$database = \Drupal\Core\Database\Database::getConnection();
    $urlString = \Drupal\Core\Database\Database::getConnectionInfoAsUrl($key);
    $databaseInfo = explode("/", $urlString);
    $database = end($databaseInfo);
    $sql = "CREATE DATABASE $database";
    //print_r($database);
    //$this->connection->createDatabase($database);
    $conn = new \PDO('mysql:host=localhost;port=3306;unix_socket=/Applications/MAMP/tmp/mysql/mysql.sock', 'root', 'root'); ///Applications/DevDesktop/mysql/bin
    $conn->setAttribute(\PDO::ATTR_ERRMODE, \PDO::ERRMODE_EXCEPTION);

    $conn->exec($sql);
  }

  /**
   * {@inheritdoc}
   */
  public function insert($table, array $values) {

  }

  /**
   * {@inheritdoc}
   */
  public function delete($table) {

  }

}
