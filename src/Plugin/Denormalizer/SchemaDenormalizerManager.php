<?php

namespace Drupal\denormalizer\Plugin\Denormalizer;

use Drupal\Core\Plugin\DefaultPluginManager;
use Drupal\Core\Cache\CacheBackendInterface;
use Drupal\Core\Extension\ModuleHandlerInterface;

/**
 * Class SchemaDenormalizerManager
 *
 * @package Drupal\denormalizer\Plugin\Denormalizer
 */
class SchemaDenormalizerManager extends DefaultPluginManager {

  /**
   * The constructor for schema denormalizer objects.
   *
   * @param \Traversable $namespaces
   *   An object that implements \Traversable which contains the root paths
   *   keyed by the corresponding namespace to look for plugin implementations.
   * @param CacheBackendInterface $cache_backend
   *   Cache backend instance to use.
   * @param ModuleHandlerInterface $module_handler
   *   The module handler to invoke the alter hook with.
   */
  public function __construct(\Traversable $namespaces, CacheBackendInterface $cache_backend, ModuleHandlerInterface $module_handler) {
    parent::__construct('Plugin/Denormalizer', $namespaces, $module_handler, 'Drupal\denormalizer\Plugin\Denormalizer\SchemaDenormalizerInterface', 'Drupal\denormalizer\Annotation\SchemaDenormalizer');
    $this->alterInfo('denormalizer_info');
  }

}
