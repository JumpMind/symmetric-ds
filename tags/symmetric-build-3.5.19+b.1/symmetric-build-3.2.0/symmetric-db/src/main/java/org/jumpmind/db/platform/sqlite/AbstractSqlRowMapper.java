package org.jumpmind.db.platform.sqlite;

import org.jumpmind.db.sql.ISqlRowMapper;

abstract public class AbstractSqlRowMapper<T> implements ISqlRowMapper<T> {

    protected boolean booleanValue(Object v) {
        if (v instanceof Integer) {
            return ((Integer) v).intValue() == 1;
        }
        return v != null && (v.equals("1") || v.equals("99"));
    }

    protected int intValue(Object v) {
        if (v != null) {
            return Integer.parseInt(v.toString());
        } else {
            return 0;
        }

    }

}