<?php

namespace Drupal\denormalizer\Plugin\Denormalizer;

/**
 * Define a concrete class for a taxonomy term field plugin denormalizer.
 *
 * @SchemaDenormalizer(
 *     id = "taxonomy_term",
 *     name = @Translation("Taxonomy Term"),
 *     hasBundle = TRUE,
 * )
 */
class TaxonomyTerm extends AbstractSchemaDenormalizer {
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
