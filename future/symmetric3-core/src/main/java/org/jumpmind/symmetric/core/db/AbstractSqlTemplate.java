package org.jumpmind.symmetric.core.db;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

abstract public class AbstractSqlTemplate implements ISqlTemplate {

    public int queryForInt(String sql) {
        return queryForObject(sql, Number.class, (Object[]) null).intValue();
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper) {
        return this.queryForCursor(sql, mapper, null, null);
    }

    public List<Map<String, Object>> query(String sql) {
        return query(sql, (Object[])null, (int[])null);
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object... args) {
        return query(sql, mapper, args, null);
    }

    public List<Map<String, Object>> query(String sql, Object[] args, int[] types) {
        return query(sql, new ISqlRowMapper<Map<String, Object>>() {
            public Map<String, Object> mapRow(java.util.Map<String, Object> row) {
                return row;
            }
        }, args, types);
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper) {
        return query(sql, mapper, null, null);
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object[] args, int[] types) {
        ISqlReadCursor<T> cursor = queryForCursor(sql, mapper, args, types);
        try {
            T next = null;
            List<T> list = new ArrayList<T>();
            do {
                next = cursor.next();
                if (next != null) {
                    list.add(next);
                }
            } while (next != null);
            return list;
        } finally {
            if (cursor != null) {
                cursor.close();
            }
        }
    }

    public int update(String sql) {
        return update(sql, null, null);
    }

    public int update(String... sql) {
        return update(true, true, -1, sql);
    }

}
