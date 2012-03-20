package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class DataLoaderServiceSqlMap extends AbstractSqlMap {

    DataLoaderServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

        // @formatter:off
        
        putSql("updateConflictSettingsSql", 
          "update $(conflict_setting) set                                                                " +
          "source_node_group_id=?, target_node_group_id=?,                                               " +
          "target_channel_id=?, target_catalog_name=?, target_schema_name=?, target_table_name=?,        " +
          "detect_update_type=?, detect_insert_type=?, detect_delete_type=?,                             " +
          "resolve_update_type=?, resolve_insert_type=?, resolve_delete_type=?, resolve_changes_only=?,  " +
          "resolve_row_only=?, detect_expression=?,                                                      " +
          "last_update_by=?, last_update_time=current_timestamp where conflict_setting_id=?              ");
        
        putSql("insertConflictSettingsSql", 
          "insert into $(conflict_setting) (                                                        " +
          "source_node_group_id, target_node_group_id,                                              " +
          "target_channel_id, target_catalog_name, target_schema_name, target_table_name,           " +
          "detect_update_type, detect_insert_type, detect_delete_type,                              " +
          "resolve_update_type, resolve_insert_type, resolve_delete_type,                           " +
          "resolve_changes_only, resolve_row_only, detect_expression,                               " +
          "create_time, last_update_by, last_update_time, conflict_setting_id)                      " +
          "values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, ?, current_timestamp, ?)");
        
        putSql("deleteConflictSettingsSql", "delete from $(conflict_setting) where conflict_setting_id=?");
        
        putSql("selectConflictSettingsSql",
                "select " +
                "source_node_group_id, target_node_group_id,                                              " +
                "target_channel_id, target_catalog_name, target_schema_name, target_table_name,           " +
                "detect_update_type, detect_insert_type, detect_delete_type,                              " +
                "resolve_update_type, resolve_insert_type, resolve_delete_type,                           " +
                "resolve_changes_only, resolve_row_only, detect_expression,                               " +
                "create_time, last_update_by, last_update_time, conflict_setting_id from $(conflict_setting) ");

        putSql("selectIncomingErrorSql",
        		"select batch_id, node_id, failed_row_number, target_catalog_name, target_schema_name, " +
        		"target_table_name, event_type, row_data, old_data, resolve_data, resolve_ignore, " +
        		"create_time, last_update_by, last_update_time from $(incoming_error)");
        
        putSql("insertIncomingErrorSql", 
        		"insert into $(incoming_error) (batch_id, node_id, failed_row_number, target_catalog_name, target_schema_name, " +
        		"target_table_name, event_type, row_data, old_data, resolve_data, resolve_ignore, " +
        		"create_time, last_update_by, last_update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");
        
        // @formatter:on
    }
}
