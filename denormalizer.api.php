<?php

/**
 * Implements hook_denormalizer_info().
 *
 * Provide a list of plain tables and entities that should be denormalized.
 */
function hook_denormalizer_info() {
  return array(
    // Non-entity
    'location' => array(
      'base table' => 'location',
    ),
    // Entity
    'denormalized_table' => array(
      'entity_type' => 'node',
      'bundles' => array('page', 'story'),
      'changed_key' => 'changed',
    ),
  );
}

/**
 * Implements hook_denormalizer_query_alter().
 *
 * Alter a denormalized query before running.
 */
function hook_denormalizer_alter(Denormalizer $d, Drupal\Core\Database\Driver\mysql\Select $q, $denormalized_view, $info) {
  if ($denormalized_view == 'denormalized_table') {
    $q->addExpression('test', 'something');
  }
}

/**
 * Implements hook_denormalizer_post_execute().
 *
 * Alter a denormalized table after running.
 */
function hook_denormalizer_post_execute($denormalized_view, $info) {
  if ($denormalized_view == 'denormalized_table') {
    //db_query('ALTER TABLE something...');
  }
}
