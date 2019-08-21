<?php

namespace Drupal\denormalizer\Plugin\Denormalizer;

use Drupal\Component\Plugin\PluginBase;
use Drupal\Core\Plugin\ContainerFactoryPluginInterface;
use Drupal\Core\StringTranslation\StringTranslationTrait;
use Drupal\denormalizer\Service\DenormalizerManagerInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * The base class for content entity schema denormailzer plugins.
 *
 * @package Drupal\denormalizer\Plugin\Denormalizer
 */
abstract class AbstractSchemaDenormalizer extends PluginBase implements SchemaDenormalizerInterface, ContainerFactoryPluginInterface {

  /**
   * {@inheritdoc}
   */
  use StringTranslationTrait;

  protected $denormalizerManager;

  public function __construct(array $configuration, string $plugin_id, $plugin_definition, DenormalizerManagerInterface $denormalizerManager) {
    parent::__construct($configuration, $plugin_id, $plugin_definition);
    $this->denormalizerManager = $denormalizerManager;
  }

  public static function create(ContainerInterface $container, array $configuration, $plugin_id, $plugin_definition) {
    return new static(
      $configuration, $plugin_id, $plugin_definition,
      $container->get('denormalizer.manager')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getMachineName() {
    return $this->pluginDefinition['id'];
  }

  /**
   * {@inheritdoc}
   */
  public function hasBundle() {
    return $this->pluginDefinition['hasBundle'];
  }

  /**
   * {@inheritdoc}
   */
  public function schemas(string $bundle = NULL) {
    if ($this->hasBundle() && !isset($bundle)) {
      throw new \Exception("Missing bundle name for '" . $this->getMachineName() . "' content entity.");
    }

    return $this->denormalizerManager->getContentEntityFieldSchema($this->getMachineName(), $bundle);
  }

}
