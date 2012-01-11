package org.jumpmind.symmetric.service.impl;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import java.util.Map;

public class ParameterServiceSqlMap extends AbstractSqlMap {

    public ParameterServiceSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) { 
        super(platform, replacementTokens);

        putSql("updateParameterSql" ,"" + 
"update $(prefixName)_parameter set param_value=? where external_id=? and node_group_id=?   " + 
"  and param_key=?                                                                          " );

        putSql("insertParameterSql" ,"" + 
"insert into $(prefixName)_parameter (external_id, node_group_id, param_key, param_value)   " + 
"  values(?, ?, ?, ?)                                                                       " );

        putSql("selectParametersSql" ,"" + 
"select param_key, param_value from $(prefixName)_parameter where external_id=? and   " + 
"  node_group_id=?                                                                    " );

        putSql("selectParametersByKeySql" ,"" + 
"select param_key, param_value, external_id, node_group_id from $(prefixName)_parameter where param_key=?   " + 
"  order by node_group_id, external_id                                                                      " );

        putSql("deleteParameterSql" ,"" + 
"delete from $(prefixName)_parameter where external_id=? and   " + 
"  node_group_id=? and param_key=?                             " );

    }

}