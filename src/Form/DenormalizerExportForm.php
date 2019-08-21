<?php

namespace Drupal\denormalizer\Form;

use Drupal\denormalizer\Denormalizer;
use Drupal\Core\Form\FormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\Core\Url;

/**
 * Defines a confirmation form to confirm denormalization.
 */
class DenormalizerExportForm extends FormBase {

  /**
   * {@inheritdoc}
   */
  public function getFormId(): string {
    return "denormalizer_export_form";
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {
    $form['header']['#markup'] = t('You can use the SQL below to generate database views of denormalized data on a copy of this database. This uses the entity property and field metadata from the current site, so queries may fail if the data is different.');

    $denormalizer = new Denormalizer();
    $denormalizer->build();
    $form['sql']['#markup'] = $denormalizer->getSql();

    $form['actions'] = [
      '#type' => 'actions',
    ];

    // Add a submit button that handles the submission of the form.
    $form['actions']['submit'] = [
      '#type' => 'submit',
      '#value' => $this->t('Submit'),
    ];
    return $form;
  }

  public function validateForm(array &$form, FormStateInterface $form_state) {
    parent::validateForm($form, $form_state);
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $messenger = \Drupal::messenger();
    $messenger->addMessage(' SQL exported');
    $form_state->setRedirect('denormalizer.settings');
  }
}
