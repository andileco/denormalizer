<?php

namespace Drupal\denormalizer\Commands;

use Drupal\denormalizer\Plugin\Denormalizer\SchemaDenormalizerManager;
use Drupal\denormalizer\Service\DenormalizerManagerInterface;
use Drush\Commands\DrushCommands;

/**
 * Class DenormalizerCommands.
 *
 * @package Drupal\denormalizer\Commands
 */
class DenormalizerCommands extends DrushCommands {

  /**
   * The denormalizer manager service.
   *
   * @var DenormalizerManagerInterface
   */
  protected $denormalizerManager;

  /**
   * The schema denormalizer plugin manager.
   *
   * @var SchemaDenormalizerManager
   */
  protected $schemaDenormalizerManager;

  /**
   * Creates a new denormalizer drush command.
   *
   * @param DenormalizerManagerInterface $denormalizerManager
   *   The denormalizer manager service
   */
  public function __construct(DenormalizerManagerInterface $denormalizerManager, SchemaDenormalizerManager $schemaDenormalizerManager) {
    $this->denormalizerManager = $denormalizerManager;
    $this->schemaDenormalizerManager = $schemaDenormalizerManager;
  }

  /**
   * Denormalize tables. Makes a delicious denormalized schema
   *
   * @command denormalizer:denormalize
   * @aliases dnz
   * @options reset Resets tables.
   * @usage drush denormalizer:denormalize --reset
   *   Resets tables.
   */
  public function denormalize($options = ['reset' => false]) {
    $entityId = 'node';
    $bundle = 'tags';

    //$schema = $this->denormalizerManager->getContentEntityFieldSchema($entityId, $bundle);
    $types = $this->denormalizerManager->getContentEntityTypes();

    $instance = $this->schemaDenormalizerManager->createInstance($entityId);
    //$instance->schemas();
    print_r($instance->schemas('page'));
    //$this->denormalizerManager->createDenormalizedTable($entityId.'_'.$bundle.'_denormalize', $schema, 'denormalizer');
    //$this->denormalizerManager->createDatabase('denormalizer');
  }

}
