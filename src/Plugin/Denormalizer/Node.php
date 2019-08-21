<?php

namespace Drupal\denormalizer\Plugin\Denormalizer;

/**
 * Define a concrete class for a node content entity.
 *
 * @SchemaDenormalizer  (
 *     id = "node",
 *     name = @Translation("Node"),
 *     hasBundle = TRUE,
 * )
 */
class Node extends AbstractSchemaDenormalizer {
  /**
   * {@inheritdoc}
   */
  /* public function schemas(string $bundle = NULL) {

    if ($this->hasBundle() && !isset($bundle)){
    throw new \Exception("Missing bundle name for '".$this->getMachineName()."' content entity.");
    }

    return $this->denormalizerManager->getContentEntityFieldSchema($this->getMachineName(), $bundle);
    } */
}
