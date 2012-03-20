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
        		"select batch_id, node_id, failed_row_number, failed_line_number, target_catalog_name, target_schema_name, " +
        		"target_table_name, event_type, row_data, old_data, resolve_data, resolve_ignore, " +
        		"create_time, last_update_by, last_update_time from $(incoming_error) where batch_id = ? and node_id = ?");

        putSql("selectCurrentIncomingErrorSql",
        		"select e.batch_id, e.node_id, e.failed_row_number, e.failed_line_number, e.target_catalog_name, e.target_schema_name, " +
        		"e.target_table_name, e.event_type, e.row_data, e.old_data, e.resolve_data, e.resolve_ignore, " +
        		"e.create_time, e.last_update_by, e.last_update_time " +
        		"from $(incoming_error) e inner join $(incoming_batch) b on b.batch_id = e.batch_id " +
        		"and b.node_id = e.batch_id and b.failed_row_number = e.failed_row_number " +
        		"where b.batch_id = ? and b.node_id = ?");

        putSql("insertIncomingErrorSql", 
        		"insert into $(incoming_error) " +
        		"(batch_id, node_id, failed_row_number, failed_line_number, target_catalog_name, target_schema_name, " +
        		"target_table_name, event_type, row_data, old_data, resolve_data, resolve_ignore, " +
        		"create_time, last_update_by, last_update_time) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)");

        putSql("updateIncomingErrorSql",
        		"update $(incoming_error) set resolve_data = ?, resolve_ignore = ? " +
        		"where batch_id = ? and node_id = ? and failed_row_number = ?");

        // @formatter:on
    }
}
