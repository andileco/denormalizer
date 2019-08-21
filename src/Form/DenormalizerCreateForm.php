<?php

namespace Drupal\denormalizer\Form;

use Drupal\denormalizer\Denormalizer;
use Drupal\Core\Form\ConfirmFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;

/**
 * Defines a confirmation form to confirm denormalization.
 */
class DenormalizerCreateForm extends ConfirmFormBase {

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    return parent::buildForm($form, $form_state);
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $denormalizer = new Denormalizer();
    $denormalizer->build();
    $denormalizer->execute();
    $config = \Drupal::service('config.factory')->getEditable('denormalizer.settings');
    $config->set('denormalizer_last_run', REQUEST_TIME);
    \Drupal::logger('denormalizer')->info('Ran denormalizer.');
  }

  /**
   * {@inheritdoc}
   */
  public function getFormId(): string {
    return "denormalizer_create_form";
  }

  /**
   * {@inheritdoc}
   */
  public function getCancelUrl() {
    //@todo this URL needs to be updated
    return new Url('denormalizer.settings');
  }

  /**
   * {@inheritdoc}
   */
  public function getQuestion() {
    return $this->t('Ready? This will create database views of normalized data.');
  }

}
