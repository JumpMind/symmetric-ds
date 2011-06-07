package org.jumpmind.symmetric.android.db;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;

abstract public class AbstractSqlRowMapper<T> implements ISqlRowMapper<T> {

    protected boolean booleanValue(Object v) {
        return v != null && v.equals("1");
    }

    protected int intValue(Object v) {
        if (v != null) {
            return Integer.parseInt(v.toString());
        } else {
            return 0;
        }

    }

}
