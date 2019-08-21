<?php

namespace Drupal\denormalizer\Plugin\Denormalizer;

use Drupal\Component\Plugin\PluginInspectionInterface;

/**
 * Defines an interface that
 *
 * @package Drupal\denormalizer\Plugin\Denormalizer
 */
interface SchemaDenormalizerInterface extends PluginInspectionInterface {

  /**
   * Gets the schemas associated with the content entity fields.
   *
   * @param string|NULL $bundle
   *   The content entity bundle machine name.
   *
   * @return array
   *   The schemas.
   */
  public function schemas(string $bundle = NULL);

  /**
   * Get the content entity machine name that needs de-normalization.
   *
   * @return string
   *   The machine name.
   */
  public function getMachineName();

  /**
   * Check whether content entity has a bundle.
   *
   * @return bool
   *   The status.
   */
  public function hasBundle();

  /**
   * Gets bundles associated with a content entity.
   *
   * @return array
   *   The content entity bundles.
   */
  //public function bundles();
}
