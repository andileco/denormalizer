<?php

namespace Drupal\denormalizer\Annotation;

use Drupal\Component\Annotation\Plugin;

/**
 * Defines a Field Denormalizer annotation object.
 *
 * Plugin Namespace: Plugin\Denormalizer
 *
 * @Annotation
 */
class SchemaDenormalizer extends Plugin {

  /**
   * The plugin ID.
   *
   * @var string
   */
  public $id;

  /**
   * The human readable of the content entity schema denormalizer.
   *
   * @var string
   */
  public $name;

  /**
   * Whether content entity associated with this schema denormalizer has a bundle.
   *
   * @var bool (optional)
   */
  public $hasBundle = FALSE;

  /**
   * The default bundles the schema denormalizer supports.
   *
   * @var array (optional)
   */
  public $bundles = [];

}
