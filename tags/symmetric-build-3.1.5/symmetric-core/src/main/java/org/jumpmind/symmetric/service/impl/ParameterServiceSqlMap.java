package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class ParameterServiceSqlMap extends AbstractSqlMap {

    public ParameterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("updateParameterSql" ,"" + 
"update $(parameter) set param_value=? where external_id=? and node_group_id=?   " + 
"  and param_key=?                                                                          " );

        putSql("insertParameterSql" ,"" + 
"insert into $(parameter) (external_id, node_group_id, param_key, param_value)   " + 
"  values(?, ?, ?, ?)                                                                       " );

        putSql("selectParametersSql" ,"" + 
"select param_key, param_value from $(parameter) where external_id=? and   " + 
"  node_group_id=?                                                                    " );

        putSql("selectParametersByKeySql" ,"" + 
"select param_key, param_value, external_id, node_group_id from $(parameter) where param_key=?   " + 
"  order by node_group_id, external_id                                                                      " );

        putSql("deleteParameterSql" ,"" + 
"delete from $(parameter) where external_id=? and   " + 
"  node_group_id=? and param_key=?                             " );

    }

}