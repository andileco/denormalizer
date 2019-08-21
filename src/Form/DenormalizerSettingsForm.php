<?php

namespace Drupal\denormalizer\Form;

use Drupal\Core\Database\Database;
use Drupal\Core\Entity\EntityTypeBundleInfoInterface;
use Drupal\Core\Form\ConfigFormBase;
use Drupal\Core\Form\FormStateInterface;
use Drupal\denormalizer\Plugin\Denormalizer\SchemaDenormalizerManager;
use Drupal\denormalizer\Service\DenormalizerManagerInterface;
use Drush\Drush;
use Drupal\Core\Config\ConfigFactoryInterface;
use Symfony\Component\DependencyInjection\ContainerInterface;

/**
 * Denormalizer Settings Form.
 */
class DenormalizerSettingsForm extends ConfigFormBase {

  /**
   * The schema denormalizer plugin manager.
   *
   * @var SchemaDenormalizerManager
   */
  protected $schemaDenormalizerManager;

  /**
   * The entity bundle service.
   *
   * @var EntityTypeBundleInfoInterface
   */
  protected $bundleInfo;

  public function __construct(ConfigFactoryInterface $configFactory, SchemaDenormalizerManager $schemaDenormalizerManager, EntityTypeBundleInfoInterface $bundleInfo) {
    parent::__construct($configFactory);
    $this->schemaDenormalizerManager = $schemaDenormalizerManager;
    $this->bundleInfo = $bundleInfo;
  }

  /**
   * {@inheritdoc}
   */
  public static function create(ContainerInterface $container) {
    return new static(
      $container->get('config.factory'),
      $container->get('plugin.manager.schema_denormalizer'),
      $container->get('entity_type.bundle.info')
    );
  }

  /**
   * {@inheritdoc}
   */
  public function getFormId() {
    return 'denormalizer_settings_form';
  }

  /**
   * {@inheritdoc}
   */
  protected function getEditableConfigNames() {
    return ['denormalizer.settings'];
  }

  /**
   * {@inheritdoc}
   */
  public function buildForm(array $form, FormStateInterface $form_state) {

    $config = $this->config('denormalizer.settings');

    $form['enabled_content_entities'] = [
      '#type' => 'details',
      '#open' => FALSE,
      '#title' => $this->t('Available content entities'),
      '#description' => $this->t('Enable content entity type to denormalize.'),
      '#tree' => TRUE,
    ];

    $contentEntities = $this->schemaDenormalizerManager->getDefinitions();

    $denormalizedContentEntities = $config->get('denormalized_content_entities');

    foreach ($contentEntities as $pluginId => $contentEntity) {
      $bundles = $this->checkBundles($pluginId);
      if (empty($bundles)) {
        $id = $pluginId;
        $form['enabled_content_entities'][$id] = array(
          '#type' => 'checkbox',
          '#title' => $contentEntity['name'],
          '#default_value' => $denormalizedContentEntities[$id],
        );
      }
      else {
        foreach ($bundles as $bundle) {
          $id = $pluginId . '_' . $bundle;
          $form['enabled_content_entities'][$id] = array(
            '#type' => 'checkbox',
            '#title' => $contentEntity['name'] . ' ' . $bundle,
            '#default_value' => $denormalizedContentEntities[$id],
          );
        }
      }
    }

    $form['sql_mode'] = [
      '#title' => $this->t('SQL mode'),
      '#type' => 'radios',
      '#options' => [
        'views' => $this->t('Create views'),
        'tables' => $this->t('Create tables'),
      ],
      '#description' => $this->t('Use views or tables for generating normalized data.'),
      '#default_value' => $config->get('sql_mode'),
    ];

    $form['db'] = [
      '#title' => $this->t('DB'),
      '#type' => 'radios',
      '#options' => [
        'local' => $this->t('Use local database'),
        'external' => $this->t('Create external database'),
      ],
      '#description' => $this->t('Which database to use'),
      '#default_value' => $config->get('db'),
    ];

    $form['view_prefix'] = [
      '#title' => $this->t('View prefix'),
      '#type' => 'textfield',
      '#default_value' => $config->get('view_prefix'),
    ];

    $form['db_prefix'] = [
      '#title' => $this->t('Database prefix'),
      '#type' => 'textfield',
      '#default_value' => $config->get('db_prefix'),
    ];

    $form['cron_enabled'] = [
      '#title' => $this->t('Run on cron?'),
      '#type' => 'checkbox',
      '#default_value' => $config->get('cron_enabled'),
      '#description' => $this->t('The denormalized views or tables will be updated incrementally on cron.'),
    ];

    $form['run_every'] = [
      '#title' => $this->t('Run every'),
      '#type' => 'textfield',
      '#default_value' => $config->get('run_every'),
      '#description' => $this->t('Time between runs.'),
    ];

    $form['reload_every'] = [
      '#title' => $this->t('Reload every'),
      '#type' => 'textfield',
      '#default_value' => $config->get('reload_every'),
      '#description' => t('Time between reloads.'),
    ];

    return parent::buildForm($form, $form_state);
  }

  public function validateForm(array &$form, FormStateInterface $form_state) {
    parent::validateForm($form, $form_state);

    $prefix = Database::getConnection()->tablePrefix();

    $no_prefix = $form_state->getValue('denormalizer_db') == 'local' && !$form_state->getValue('denormalizer_view_prefix');
    $same_prefix = $form_state->getValue('denormalizer_db') == 'local' && $form_state->getValue('denormalizer_view_prefix') == $prefix;
    if ($no_prefix || $same_prefix) {
      $form_state->setErrorByName('denormalizer_view_prefix', $this->t('Using the local database requires a view prefix. Otherwise your tables will be overwritten!'));
    }

    if ($form_state->getValue('denormalizer_db') == 'external' && !$form_state->getValue('denormalizer_db_prefix')) {
      $form_state->setErrorByName('denormalizer_db_prefix', $this->t('Database prefix is required if using an external database.'));
    }
  }

  /**
   * {@inheritdoc}
   */
  public function submitForm(array &$form, FormStateInterface $form_state) {
    $this->config('denormalizer.settings')
      ->setData($form_state->getValues())
      ->save();
    parent::submitForm($form, $form_state);
  }

  private function checkBundles($entityId) {
    $bundles = $this->bundleInfo->getBundleInfo($entityId);

    if (array_key_exists($entityId, $bundles)) {

      return [];
    }

    return array_keys($bundles);
  }

}
