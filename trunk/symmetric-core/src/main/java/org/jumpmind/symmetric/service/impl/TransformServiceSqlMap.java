package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class TransformServiceSqlMap extends AbstractSqlMap {

    public TransformServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("selectTransformTable" ,"" + 
"select                                                        " + 
"  transform_id, source_node_group_id, target_node_group_id,   " + 
"  source_catalog_name, source_schema_name,                    " + 
"  source_table_name,                                          " + 
"  target_catalog_name, target_schema_name,                    " + 
"  target_table_name,                                          " + 
"  transform_point,                                            " + 
"  transform_order,                                            " + 
"  update_first, delete_action, column_policy,                 " +
"  last_update_time, last_update_by, create_time               " + 
"  from                                                        " + 
"  $(transform_table) order by transform_order                 " + 
"  asc                                                         " );

        putSql("selectTransformColumn" ,"" + 
"select                                            " + 
"  transform_id, include_on, target_column_name,   " + 
"  source_column_name, pk,                         " + 
"  transform_type, transform_expression,           " + 
"  transform_order,                                " +
"  last_update_time, last_update_by, create_time   " + 
"  from $(transform_column) order                  " + 
"  by transform_order                              " + 
"  asc                                             " );

        putSql("selectTransformColumnForTable" ,"" + 
"select                                            " + 
"  transform_id, include_on, target_column_name,   " + 
"  source_column_name, pk,                         " + 
"  transform_type, transform_expression,           " + 
"  transform_order,                                " +
"  last_update_time, last_update_by, create_time   " + 
"  from $(transform_column)                        " + 
"  where                                           " + 
"  transform_id = ?                                " + 
"  order by transform_order asc                    " );

        putSql("updateTransformTableSql" ,"" + 
"update                       " + 
"  $(transform_table)   " + 
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
"  column_policy=?,           " +
"  last_update_time=?,        " +
"  last_update_by=?           " +
"  where                      " + 
"  transform_id=?             " );

        putSql("updateTransformColumnSql" ,"" + 
"update                        " + 
"  $(transform_column)   " + 
"  set                         " + 
"  source_column_name=?,       " + 
"  pk=?,                       " + 
"  transform_type=?,           " + 
"  transform_expression=?,     " + 
"  transform_order=?,           " +
"  last_update_time=?,        " +
"  last_update_by=?           " +
"  where                       " + 
"  transform_id=?              " + 
"  and include_on=?            " + 
"  and                         " + 
"  target_column_name=?        " );

        putSql("insertTransformTableSql" ,"" + 
"insert into $(transform_table)                                  " + 
"  (source_node_group_id, target_node_group_id, source_catalog_name,   " + 
"  source_schema_name, source_table_name,                              " + 
"  target_catalog_name, target_schema_name, target_table_name,         " + 
"  transform_point, update_first, delete_action, transform_order,      " + 
"  column_policy, last_update_time, last_update_by, create_time, transform_id)                                        " + 
"  values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)                           " );

        putSql("insertTransformColumnSql" ,"" + 
"insert into $(transform_column)              " + 
"  (transform_id, include_on, target_column_name,   " + 
"  source_column_name,                              " + 
"  pk, transform_type,                              " + 
"  transform_expression, transform_order, " +
"  last_update_time, last_update_by, create_time)           " + 
"  values(?,?,?,?,?,?,?,?,?,?,?)                          " );

        putSql("deleteTransformTableSql" ,"" + 
"delete from $(transform_table) where   " + 
"  transform_id=?                             " );

        putSql("deleteTransformColumnsSql" ,"" + 
"delete from $(transform_column) where   " + 
"  transform_id=?                              " );

        putSql("deleteTransformColumnSql" ,"" + 
"delete from $(transform_column)   " + 
"  where                                 " + 
"  transform_id=?                        " + 
"  and include_on=?                      " + 
"  and target_column_name=?              " );

    
    
        putSql("selectMaxTransformTableLastUpdateTime" ,"select max(last_update_time) from $(transform_table) where last_update_time is not null" );       
        putSql("selectMaxTransformColumnLastUpdateTime" ,"select max(last_update_time) from $(transform_column) where last_update_time is not null" );
    
    }
}