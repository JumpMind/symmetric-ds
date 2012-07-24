package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class LoadFilterServiceSqlMap extends AbstractSqlMap {

    public LoadFilterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("selectLoadFilterTable" ,"" + 
"select                                                               " + 
"  load_filter_id, load_filter_type,                                  " +
"  source_node_group_id, target_node_group_id,                        " + 
"  target_catalog_name, target_schema_name,                           " + 
"  target_table_name,                                                 " +
"  filter_on_insert, filter_on_update, filter_on_delete,              " +
"  before_write_script, after_write_script,                           " +
"  batch_complete_script, batch_commit_script, batch_rollback_script, " +
"  create_time, last_update_by, last_update_time,                     " +
"  load_filter_order, fail_on_error                                   " +
"  from                                                               " + 
"  $(load_filter) order by load_filter_order                          "); 

        putSql("updateLoadFilterSql" ,"" + 
"update                       " + 
"  $(load_filter)   " + 
"  set                        " + 
"  source_node_group_id=?,    " + 
"  target_node_group_id=?,    " + 
"  source_catalog_name=?,     " + 
"  source_schema_name=?,      " + 
"  source_table_name=?,       " + 
"  target_catalog_name=?,     " + 
"  target_schema_name=?,      " + 
"  target_table_name=?,       " + 
"  transform_point=?,         " + 
"  update_first=?,            " + 
"  delete_action=?,           " + 
"  transform_order=?,         " + 
"  column_policy=?            " +
"  where                      " + 
"  transform_id=?             " );

        putSql("insertLoadFilterSql" ,"" + 
"insert into $(transform_table)                                  " + 
"  (source_node_group_id, target_node_group_id, source_catalog_name,   " + 
"  source_schema_name, source_table_name,                              " + 
"  target_catalog_name, target_schema_name, target_table_name,         " + 
"  transform_point, update_first, delete_action, transform_order,      " + 
"  column_policy, transform_id)                                        " + 
"  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?)                                 " );

        putSql("deleteLoadFilterSql" ,"" + 
"delete from $(load_filter) where   " + 
"  load_filter_id=?                             " );

    }

}