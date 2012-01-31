package org.jumpmind.symmetric.service.impl;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.util.FormatUtils;

/**
 * Utility SQL container that should be sub-classed in order to populate with
 * SQL statements from the subclasses constructor.
 */
abstract public class AbstractSqlMap implements ISqlMap {

    private IDatabasePlatform platform;

    private Map<String, String> sql = new HashMap<String, String>();

    protected Map<String, String> replacementTokens;

    public AbstractSqlMap(IDatabasePlatform platform, Map<String, String> replacementTokens) {
        this.platform = platform;
        this.replacementTokens = replacementTokens;
    }

    protected void putSql(String key, String sql) {
        if (replacementTokens != null) {
            sql = FormatUtils.replaceTokens(sql, this.replacementTokens, true);
        }

        this.sql.put(key, this.platform != null ? this.platform.scrubSql(sql) : sql);
    }

    public String getSql(String... keys) {
        StringBuilder sqlBuffer = new StringBuilder();
        if (keys != null) {
            if (keys.length > 1) {
                for (String key : keys) {
                    if (key != null) {
                        String value = sql.get(key);
                        sqlBuffer.append(value == null ? key : value);
                    }
                }
            } else if (keys.length == 1) {
                sqlBuffer.append(sql.get(keys[0]));
            }
        }
        return sqlBuffer.toString();
    }

}
