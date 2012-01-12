package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;

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
"  update_first, delete_action                                 " + 
"  from                                                        " + 
"  $(prefixName)_transform_table order by transform_order           " + 
"  asc                                                         " );

        putSql("selectTransformColumn" ,"" + 
"select                                            " + 
"  transform_id, include_on, target_column_name,   " + 
"  source_column_name, pk,                         " + 
"  transform_type, transform_expression,           " + 
"  transform_order                                 " + 
"  from $(prefixName)_transform_column order            " + 
"  by transform_order                              " + 
"  asc                                             " );

        putSql("selectTransformColumnForTable" ,"" + 
"select                                            " + 
"  transform_id, include_on, target_column_name,   " + 
"  source_column_name, pk,                         " + 
"  transform_type, transform_expression,           " + 
"  transform_order                                 " + 
"  from $(prefixName)_transform_column                  " + 
"  where                                           " + 
"  transform_id = ?                                " + 
"  order by transform_order asc                    " );

        putSql("updateTransformTableSql" ,"" + 
"update                       " + 
"  $(prefixName)_transform_table   " + 
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
"  transform_order=?          " + 
"  where                      " + 
"  transform_id=?             " );

        putSql("updateTransformColumnSql" ,"" + 
"update                        " + 
"  $(prefixName)_transform_column   " + 
"  set                         " + 
"  source_column_name=?,       " + 
"  pk=?,                       " + 
"  transform_type=?,           " + 
"  transform_expression=?,     " + 
"  transform_order=?           " + 
"  where                       " + 
"  transform_id=?              " + 
"  and include_on=?            " + 
"  and                         " + 
"  target_column_name=?        " );

        putSql("insertTransformTableSql" ,"" + 
"insert into $(prefixName)_transform_table                                  " + 
"  (source_node_group_id, target_node_group_id, source_catalog_name,   " + 
"  source_schema_name, source_table_name,                              " + 
"  target_catalog_name, target_schema_name, target_table_name,         " + 
"  transform_point, update_first, delete_action, transform_order,      " + 
"  transform_id)                                                       " + 
"  values(?,?,?,?,?,?,?,?,?,?,?,?,?)                                   " );

        putSql("insertTransformColumnSql" ,"" + 
"insert into $(prefixName)_transform_column              " + 
"  (transform_id, include_on, target_column_name,   " + 
"  source_column_name,                              " + 
"  pk, transform_type,                              " + 
"  transform_expression, transform_order)           " + 
"  values(?,?,?,?,?,?,?,?)                          " );

        putSql("deleteTransformTableSql" ,"" + 
"delete from $(prefixName)_transform_table where   " + 
"  transform_id=?                             " );

        putSql("deleteTransformColumnsSql" ,"" + 
"delete from $(prefixName)_transform_column where   " + 
"  transform_id=?                              " );

        putSql("deleteTransformColumnSql" ,"" + 
"delete from $(prefixName)_transform_column   " + 
"  where                                 " + 
"  transform_id=?                        " + 
"  and include_on=?                      " + 
"  and target_column_name=?              " );

    }

}