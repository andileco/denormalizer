<?php

use Drupal\denormalizer\Denormalizer;

function denormalizer_drush_help($section) {
  switch ($section) {
    case 'drush:denormalize':
      return dt("Denormalize schema.");
  }
}

function denormalizer_drush_command() {
  $items = array();

  $items['denormalize'] = array(
    'description' => 'makes a delicious denormalized schema ',
    'bootstrap' => DRUSH_BOOTSTRAP_DRUPAL_FULL,
    'options' => array(
      'reset' => 'Resets tables.',
    ),
  );
  return $items;
}

/**
 * Denormalize tables.
 */
function drush_denormalizer_denormalize() {
  $reset = (bool) drush_get_option('reset');
  $d = new Denormalizer();
  $d->build();
  $d->execute($reset);
}
