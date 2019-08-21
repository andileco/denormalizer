<?php

namespace Drupal\denormalizer;

use Drupal\Component\Utility\Unicode;
use Drupal\Core\Database\Database;


class Denormalizer {

  // The DN tables to be exported/created.
  private $dw_tables = [];

  /**
   * Generate all SQL for views.
   *
   * @return array
   *   Array of SQL select statements.
   */
  function build() {
    $databases = Database::getAllConnectionInfo();
    $other_database = $databases['default']['default'];
    $other_database['prefix'] = '';
    Database::addConnectionInfo('external', 'default', $other_database);

    $d = denormalizer_get_info();

    foreach ($d as $denormalizer_view => $dn_info) {
      $this->add($denormalizer_view, $dn_info);
    }
  }

  /**
   * Add a table to the denormalizer...list?
   *
   * @param type $denormalizer_view
   *   Name of the view.
   *
   * @param array $dn_info
   *   Info bit from hook_denormalizer_info().
   */
  function add($denormalizer_view, $dn_info) {
    $query = $this->generateQuery($denormalizer_view, $dn_info);
    $this->dw_tables[$denormalizer_view] = $query;
  }

  /**
   * Convert
   */

  //  function denom_convert_schema2($property_info) {
  //
  //    foreach ($property_info as $index => $info) {
  //      $type = '';
  //
  //      if (strpos($info->getType(), 'integer') !== FALSE || strpos($info->getType(), 'boolean') !== FALSE || $index == 'uid') {
  //        $type = 'integer';
  //      }
  //      elseif (strpos($info->getType(), 'string') !== FALSE || $index == 'uuid') {
  //        $type = 'text';
  //      }
  //      else {
  //        $type = 'text';
  //      }
  //
  //      if ($type) {
  //        $properties[$info->getName()] = [
  //          'type' => $type,
  //          'label' => Unicode::ucfirst($info->getName()),
  //          'schema field' => $info->getName(),
  //          // As we cannot know about any setter access, leave out the setter
  //          // callback. For getting usually no further access callback is needed.
  //        ];
  //        if ($info->getType() == 'serial') {
  //          $properties[$info->getName()]['validation callback'] = 'entity_metadata_validate_integer_positive';
  //        }
  //      }
  //
  //    }
  //
  //    return $properties;
  //  }

  /**
   * Create a view.
   *
   * @param string $denormalizer_view
   *   Name of view to create.
   * @param string $entity_type
   *   Entity type to create from.
   * @param array<string> $bundles
   *   Bundles to include. Blank for all bundles (all entity fields).
   *
   * @return string
   *   SQL to use to pull denormalized data or create a view.
   */
  function generateQuery($denormalizer_view, $dn_info) {

    $d = denormalizer_get_info();
    $schema = drupal_get_module_schema($d[$denormalizer_view]['schema_module'], $d[$denormalizer_view]['base table']);
    $property_info = [];
    if (isset($dn_info['entity_type'])) {
      $property_info = \Drupal::service('entity_field.manager')
        ->getBaseFieldDefinitions($dn_info['entity_type']);
      $base_fields = [];
      $injected_database = Database::getConnection();
      $resuls = $injected_database->query("SHOW COLUMNS FROM " . $dn_info['entity_type'] . ";");
      $base_columns = [];
      foreach ($resuls as $index => $info) {
        array_push($base_columns, $info->Field);
        array_push($base_fields, $info->Field);
      }
      $resuls = $injected_database->query("SHOW COLUMNS FROM " . $dn_info['entity_type'] . "_field_data;");
      $base_node_field = [];
      foreach ($resuls as $index => $info) {
        array_push($base_node_field, $info->Field);
        if (!in_array($info->Field, $base_fields, TRUE)) {
          array_push($base_fields, $info->Field);
        }
      }
    }
    else {
      if (empty($dn_info['external'])) {

        $property_info = $this->denorm_metadata_convert_schema($dn_info['base table']);

      }
    }

    $base_table = \Drupal::entityTypeManager()
      ->getDefinition('node')
      ->getBaseTable();

    $injected_database = Database::getConnection();

    $q = $injected_database->select($base_table, $denormalizer_view);

    $q->leftJoin($base_table . '_field_data', 'field_data', "field_data.nid = $denormalizer_view.nid");

    if (!empty($dn_info['external'])) {
      $q->fields($denormalizer_view);
    }

    $properties_columns = [];
    foreach ($property_info as $property => $info) {
      if ($info['schema field']) {
        if (in_array($info['schema field'], [
          'revision_timestamp',
          'revision_uid',
          'revision_log',
          'revision_default',
          'path',
          'menu_link',
        ])) {
          // ??? this column does not exist
          continue;
        }

        if (!empty($schema[$dn_info['base table']]['fields'][$info['schema field']]['serialize'])) {
          // No serialized fields.
          continue;
        }

        $properties_columns[$info['schema field']] = $info['schema field'];
        if (isset($info['type']) && $info['type'] == 'date') {
          $q->addExpression("from_unixtime($denormalizer_view.{$info['schema field']})", "{$info['schema field']}");
        }
        else {
          // @todo: How do we get column values for base fields and also field API fields?
          //$baseColumnName = $info->getFieldStorageDefinition()->getMainPropertyName();

          // print_r($property." ");
          $q->fields($denormalizer_view, [$property]);
        }
      }
      else {

        //todo handle serializable and date fields

        if (in_array($property, $base_node_field) && in_array($property, $base_columns)) {
          $q->fields($denormalizer_view, [$property]);
        }
        elseif (in_array($property, $base_node_field)) {
          $q->fields('field_data', [$property]);
        }
        elseif (in_array($property, $base_columns)) {
          $q->fields($denormalizer_view, [$property]);
        }
        else {
          // ??? this column does not exist
          continue;
        }
      }
    }

    // Track fields we already processed :
    $used = [];
    if (isset($dn_info['entity_type']) && $dn_info['fields'] !== FALSE) {
      $entity_type = $dn_info['entity_type'];
      $entity_info = $dn_info;
      $bundles = $dn_info['bundles'][0];

      //todo find the way to manage bundle if empty
      //todo need another way to test all bundles
      $fields = \Drupal::entityManager()
        ->getFieldDefinitions($dn_info['entity_type'], $bundles);
      $entity_info = \Drupal::entityTypeManager()
        ->getDefinition($dn_info['entity_type']);

      foreach ($fields as $field_name => $info) {
        if (in_array($field_name, $base_fields, TRUE)) {
          //?? this columns allready processed
          continue;
        }

        if (in_array($field_name, [
          'revision_timestamp',
          'revision_uid',
          'revision_log',
          'revision_default',
          'path',
          'menu_link',
        ])) {
          //?? this columns do not exist
          continue;
        }

        if (!isset($used[$field_name])) {
          $used[$field_name] = $field_name;
          if (!$fields[$field_name]->getFieldStorageDefinition()
            ->isMultiple()) {
            $cols = [];
            if ($fields[$field_name]->getType() == 'date') {

              // "Date" time, we need to special handle :( Cast into a real
              // datetime so ETL tools can pick it up.
              $q->addJoin('LEFT', 'node__' . $field_name, $field_name, "$field_name.entity_id = $denormalizer_view.{$entity_info->getKey('id')}", []);
              $col1 = $field_name . '_' . 'value';
              $q->addExpression("cast($col1 as datetime)", $col1);
              if (!empty($fields[$field_name]['settings']['todate'])) {
                $col2 = $field_name . '_' . 'value2';
                $q->addExpression("cast($col2 as datetime)", $col2);
              }
              // Skip this field.
              continue;
            }

            foreach ($fields[$field_name]->getFieldStorageDefinition()
                       ->getColumns() as $column => $info) {
              // No full text fields.
              if ($fields[$field_name]->getType() == 'text_long' || $fields[$field_name]->getType() == 'text_with_summary') {
                continue;
              }
              $cols[] = $field_name . '_' . $column;
            }

            //todo add entity_type condition like D7 ??
            if (!empty($cols)) {
              // Join field table. Add columns to query.
              $q->addJoin('LEFT', 'node__' . $field_name, $field_name, "$field_name.entity_id = $denormalizer_view.{$entity_info->getKey('id')}", []);
              $q->fields($field_name, $cols);
            }
          }
          elseif ($fields[$field_name]['type'] == 'taxonomy_term_reference') {
            // Handle taxonomy terms. We get the values and concatenate them into one column.
            $q->addJoin('LEFT', 'node__' . $field_name, $field_name, "$field_name.entity_id = $denormalizer_view.{$entity_info->getKey('id')}", []);
            $q->addJoin('LEFT', 'taxonomy_term_data', "{$field_name}_tax", "{$field_name}_tid = {$field_name}_tax.tid");
            $q->addExpression("group_concat(distinct {$field_name}_tax.name SEPARATOR '|')", "{$field_name}_tid");
            // Group by primary key, so group_concat will work.
            $q->groupBy("$denormalizer_view.{$entity_info->getKey('id')}");
          }
          elseif ($fields[$field_name]['cardinality'] != 1) {
            if (in_array($fields[$field_name]['module'], ['list'])) {
              $q->addJoin('LEFT', 'node__' . $field_name, $field_name, "$field_name.entity_id = $denormalizer_view.{$entity_info->getKey('id')}", []);
              $q->addExpression("group_concat(distinct {$field_name}_value SEPARATOR '|')", "{$field_name}_value");
              // Group by primary key, so group_concat will work.
              $q->groupBy("$denormalizer_view.{$entity_info->getKey('id')}");
            }
          }
        }
      }

      // Filter on bundle.
      if (count($bundles) > 0) {
        /** @todo fix me */
        // $q->condition("dn_$denormalizer_view.{$entity_info['entity keys']['bundle']}", $bundles);
      }
    }

    return $q;
  }

  /**
   * Converts the schema information available for the given table to property
   * info.
   *
   * @param $table
   *   The name of the table as used in hook_schema().
   *
   * @return
   *   An array of property info as suiting for hook_entity_property_info().
   */
  function denorm_metadata_convert_schema($table) {

    //$schema = StorableConfigBase::getSchemaWrapper();
    $properties = [];
    //todo Find the best way to get the schema ??
    $injected_database = Database::getConnection();
    $result = $injected_database->query("SHOW COLUMNS FROM " . $table . ";");

    foreach ($result as $index => $info) {
      $type = '';
      if (strpos($info->Type, 'int') !== FALSE || strpos($info->Type, 'serial') !== FALSE || strpos($info->Type, 'date') !== FALSE) {
        $type = 'integer';
      }
      elseif (strpos($info->Type, 'float') !== FALSE || strpos($info->Type, 'numeric') !== FALSE) {
        $type = 'decimal';
      }
      elseif (strpos($info->Type, 'char') !== FALSE || strpos($info->Type, 'varchar') !== FALSE || strpos($info->Type, 'text') !== FALSE) {
        $type = 'text';
      }

      if ($type) {
        $properties[$info->Field] = [
          'type' => $type,
          'label' => Unicode::ucfirst($info->Field),
          'schema field' => $info->Field,
          // As we cannot know about any setter access, leave out the setter
          // callback. For getting usually no further access callback is needed.
        ];
        if ($info->Type == 'serial') {
          $properties[$info->Field]['validation callback'] = 'entity_metadata_validate_integer_positive';
        }
      }
    }
    return $properties;
  }

  /**
   * Create all database views.
   *
   * @param bool $reset
   *   Whether or not to drop and recreate tables.
   */
  function execute($reset = FALSE) {
    $config = \Drupal::service('config.factory')
      ->getEditable('denormalizer.settings');

    $prefix = $config->get('view_prefix', 'snowflake_');
    $db_prefix = '';
    $db_target = denormalizer_target_db();

    if ($db = denormalizer_target_db()) {
      $db_prefix = "{$db}.";
      $injected_database = Database::getConnection();
      $target_exists = $injected_database->query('SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :name', [':name' => $db_target])
        ->fetchField();
      //$target_exists = db_query('SELECT 1 FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = :name', array(':name' => $db_target))->fetchField();
      if ($reset) {
        if ($target_exists) {
          $injected_database = Database::getConnection();
          $injected_database->query("DROP DATABASE $db_target");
          //db_query("DROP DATABASE $db_target");
          drupal_set_message(t('Dropped %target database.', ['%target' => $db_target]));
          $target_exists = FALSE;
        }
      }

      if (!$target_exists) {
        $injected_database = Database::getConnection();
        $injected_database->query("CREATE DATABASE $db_target");
        //db_query("CREATE DATABASE $db_target");
        drupal_set_message(t('Created %target database.', ['%target' => $db_target]));
      }
    }

    if ($config->get('sql_mode', 'views') == 'views') {
      $type = 'VIEW';
    }
    else {
      $type = 'TABLE';
    }

    $out = [];
    $dn_info = denormalizer_get_info();
    $all_start = microtime(TRUE);

    // Hack to get around https://stackoverflow.com/questions/36882149/error-1067-42000-invalid-default-value-for-created-at
    // Some tables have a CURRENT_TIMESTAMP so the derived table gets a zero date.
    $injected_database = Database::getConnection();
    $injected_database->query("SET sql_mode = 'REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER'");
    //db_query("SET sql_mode = 'REAL_AS_FLOAT,PIPES_AS_CONCAT,ANSI_QUOTES,IGNORE_SPACE,ERROR_FOR_DIVISION_BY_ZERO,NO_AUTO_CREATE_USER'");

    // A little more flexibility for handling multiple values.
    $group_concat_max_len = intval($config->get('group_concat_max_len', 16384));
    $injected_database = Database::getConnection();
    $injected_database->query("SET group_concat_max_len = $group_concat_max_len");
    //db_query("SET group_concat_max_len = $group_concat_max_len");

    foreach ($this->dw_tables as $denormalizer_view => $final_q) {
      $target = "{$db_prefix}" . "`$prefix$denormalizer_view`";
      $start = microtime(TRUE);
      $id_key = denormalizer_get_primary_key($denormalizer_view);

      $injected_database = Database::getConnection();

      //$reset = ($type == 'VIEW') || !db_query('SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name', array(':schema' => $db_target, ':name' => $denormalizer_view))->fetchField();
      $reset = ($type == 'VIEW') || !$injected_database->query('SELECT 1 FROM INFORMATION_SCHEMA.TABLES WHERE TABLE_SCHEMA = :schema AND TABLE_NAME = :name', [
          ':schema' => $db_target,
          ':name' => $denormalizer_view,
        ])->fetchField();

      if (empty($reset)) {
        if (!$config->get('max_changed_' . $denormalizer_view)) {
          // No reset, but we can't reload.
          $injected_database = Database::getConnection();
          $count = $injected_database->query("SELECT COUNT(*) FROM $target")
            ->fetchField();
          //$count = db_query("SELECT COUNT(*) FROM $target")->fetchField();
          if ($count > 0) {
            $injected_database = Database::getConnection();
            $injected_database->query("TRUNCATE $target");
            //db_query("TRUNCATE $target");
            $out[] = t('Emptied :target.', [':target' => $target]);
          }
        }
        else {
          if (isset($dn_info[$denormalizer_view]['changed_key']) && $config->get('max_changed_' . $denormalizer_view)) {
            // If there is a last changed key, only replicate records that have changed.
            $final_q->havingCondition($dn_info[$denormalizer_view]['changed_key'], $config->get('max_changed_' . $denormalizer_view), '>');
          }
        }

        // Allow altering query before update.
        \Drupal::moduleHandler()
          ->alter('denormalizer', $final_q, $denormalizer_view, $dn_info[$denormalizer_view]);

        if (!empty($dn_info[$denormalizer_view]['external'])) {
          db_set_active('external');
        }

        $final_sql = denormalizer_dpq($final_q);
        db_set_active();
        try {
          $injected_database = Database::getConnection();
          $affected = $injected_database->query("REPLACE INTO {$db_prefix}$prefix{$denormalizer_view} $final_sql");
          //$affected = db_query("REPLACE INTO {$db_prefix}$prefix{$denormalizer_view} $final_sql");
          $affected->allowRowCount = TRUE;
          $count = $affected->rowCount();
        } catch (Exception $e) {
          drupal_set_message(t('Could not denormalize @table: @message', [
            '@table' => $denormalizer_view,
            '@message' => $e->getMessage(),
          ]), 'error');
        }
        $end = microtime(TRUE);
        $time = round($end - $start, 2);
        $out[] = t('Loaded @records to %view in @time seconds.', [
          '%view' => "{$db_prefix}$prefix{$denormalizer_view}",
          '@records' => $affected,
          '@time' => $time,
        ]);
      }
      else {
        $injected_database = Database::getConnection();
        $injected_database->query("DROP TABLE IF EXISTS {$db_prefix}$prefix$denormalizer_view");
        $injected_database->query("DROP VIEW IF EXISTS {$db_prefix}$prefix$denormalizer_view");
        //db_query("DROP TABLE IF EXISTS {$db_prefix}$prefix$denormalizer_view");
        //db_query("DROP VIEW IF EXISTS {$db_prefix}$prefix$denormalizer_view");
        //$out[] = t('Dropped %name.', array('%name' => "{$db_prefix}$prefix$denormalizer_view"));

        if (!empty($dn_info[$denormalizer_view]['external'])) {
          db_set_active('external');
        }
        // Allow altering query before insert.
        \Drupal::moduleHandler()
          ->alter('denormalizer', $final_q, $denormalizer_view, $dn_info[$denormalizer_view]);
        $final_sql = denormalizer_dpq($final_q);
        db_set_active();

        if ($type == 'TABLE') {
          // Create an empty table.
          // https://www.percona.com/blog/2018/01/10/why-avoid-create-table-as-select-statement/
          $injected_database = Database::getConnection();
          $affected = $injected_database->query("CREATE $type $target CHARACTER SET utf8 COLLATE utf8_general_ci ENGINE = MYISAM AS $final_sql LIMIT 0");
          //$affected = db_query("CREATE $type $target CHARACTER SET utf8 COLLATE utf8_general_ci ENGINE = MYISAM AS $final_sql LIMIT 0");
          $affected->allowRowCount = TRUE;
          $count = $affected->rowCount();
          if ($id_key) {
            $injected_database = Database::getConnection();
            $injected_database->query("ALTER TABLE $target ADD PRIMARY KEY $id_key ($id_key)");
            //db_query("ALTER TABLE $target ADD PRIMARY KEY $id_key ($id_key)");
          }
          // Insert records into it.
          try {
            $injected_database = Database::getConnection();
            $affected = $injected_database->query("INSERT INTO $target $final_sql");
            //$affected = db_query("INSERT INTO $target $final_sql");
            $affected->allowRowCount = TRUE;
            $count = $affected->rowCount();
          } catch (Exception $e) {
            drupal_set_message(t('Could not denormalize @table: @message', [
              '@table' => $denormalizer_view,
              '@message' => $e->getMessage(),
            ]), 'error');
          }
        }
        elseif ($type == 'VIEW') {
          try {
            $injected_database = Database::getConnection();

            //print_r("Final : ".$final_sql);

            $affected = $injected_database->query("CREATE $type $target AS $final_sql");
            //$affected = db_query("CREATE $type $target AS $final_sql");
            $affected->allowRowCount = TRUE;
            $count = $affected->rowCount();
          } catch (Exception $e) {
            drupal_set_message(t('Could not denormalize @table: @message', [
              '@table' => $denormalizer_view,
              '@message' => $e->getMessage(),
            ]), 'error');
          }
        }

        $end = microtime(TRUE);
        $time = round($end - $start, 2);
        $out[] = t('Created @type %view with @records records in @time seconds.', [
          '@type' => $type,
          '%view' => "{$db_prefix}$prefix{$denormalizer_view}",
          '@records' => $affected,
          '@time' => $time,
        ]);
      }

      // Log the last ID and last changed value.
      if (isset($dn_info['base table'])) {
        $base_table = $dn_info['base table'];
        //print_r($dn_info[$denormalizer_view]['base table']);
      }
      else {
        //todo get the entity type from dn_info
        $base_table = \Drupal::entityTypeManager()
          ->getDefinition('node')
          ->getBaseTable();
      }

      if ($type == 'TABLE') {
        if (isset($dn_info[$denormalizer_view]['changed_key'])) {
          if (!empty($dn_info[$denormalizer_view]['external'])) {
            db_set_active('external');
          }
          $injected_database = Database::getConnection();
          $changed = $injected_database->query("SELECT max({$dn_info[$denormalizer_view]['changed_key']}) from $target")
            ->fetchField();
          //$changed = db_query("SELECT max({$dn_info[$denormalizer_view]['changed_key']}) from $target")->fetchField();
          db_set_active();
          \Drupal::service('config.factory')
            ->getEditable('denormalizer_max_changed_' . $denormalizer_view, $changed);
        }
      }

      $args = [$denormalizer_view, $dn_info[$denormalizer_view]];
      \Drupal::moduleHandler()->invokeAll('denormalizer_post_execute', $args);
    }
    $all_end = microtime(TRUE);
    $all_time = round($all_end - $all_start, 2);
    $out[] = t('Created all in @time seconds.', ['@time' => $all_time]);
    drupal_set_message(implode("<br/>", $out));
  }

  /**
   * Show full SQL used to create all database items.
   */
  function getSql() {
    $config = \Drupal::service('config.factory')
      ->getEditable('denormalizer.settings');
    $db_source = denormalizer_source_db();
    $sql = '';


    if ($config->get('db') == 'external') {
      $db_target = denormalizer_target_db();
      $db_prefix = "{$db_target}.";
      $sql .= "CREATE DATABASE IF NOT EXISTS $db_target;\n\n";
    }


    if ($config->get('sql_mode', 'views') == 'views') {
      $type = 'VIEW';
    }
    else {
      $type = 'TABLE';
    }

    $prefix = $config->get('view_prefix', 'snowflake_');


    $dn_info = denormalizer_get_info();
    foreach ($this->dw_tables as $denormalizer_view => $final_q) {
      if (!empty($dn_info[$denormalizer_view]['external'])) {
        db_set_active('external');
      }

      // Allow altering query before insert.
      \Drupal::moduleHandler()
        ->alter('denormalizer', $final_q, $denormalizer_view, $dn_info[$denormalizer_view]);

      $final_sql = denormalizer_dpq($final_q);
      $target = "{$db_prefix}{$prefix}{$denormalizer_view}";
      // Drop views and tables.
      $sql .= "DROP TABLE IF EXISTS $target;\n";
      $sql .= "DROP VIEW IF EXISTS $target;\n";
      $sql .= "CREATE $type $target AS $final_sql;\n";
      if ($key = denormalizer_get_primary_key($denormalizer_view)) {
        $sql .= "ALTER TABLE $target ADD PRIMARY KEY $key ($key);\n\n";
      }
      // Reset to default DB.
      db_set_active();
    }

    $page = [];

    $page['header']['#markup'] = t('You can use the SQL below to generate database views of denormalized data on a copy of this database. This uses the entity property and field metadata from the current site, so queries may fail if the data is different.');

    $page['sql']['#type'] = 'textarea';
    $page['sql']['#rows'] = '100';
    $page['sql']['#value'] = $sql;

    return drupal_render($page);
  }

}
