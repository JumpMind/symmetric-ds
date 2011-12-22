package org.jumpmind.db.sql;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.DmlStatement.DmlType;

abstract public class AbstractSqlTemplate implements ISqlTemplate {

    protected boolean dateOverrideToTimestamp;

    protected String identifierQuoteString;

    public int queryForInt(String sql) {
        return queryForObject(sql, Number.class, (Object[]) null).intValue();
    }

    public <T> ISqlReadCursor<T> queryForCursor(Query query, ISqlRowMapper<T> mapper) {
        return this.queryForCursor(query.getSql(), mapper, query.getArgs(), query.getArgTypes());
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper) {
        return this.queryForCursor(sql, mapper, null, null);
    }

    public List<Row> query(String sql) {
        return query(sql, (Object[]) null, (int[]) null);
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object... args) {
        return query(sql, mapper, args, null);
    }

    public <T> List<T> query(Query query, ISqlRowMapper<T> mapper) {
        return query(query.getSql(), mapper, query.getArgs(), query.getArgTypes());
    }

    @SuppressWarnings("unchecked")
    public <T, W> Map<T, W> query(String sql, String keyCol, String valueCol, Object[] args,
            int[] types) {
        List<Row> rows = query(sql, args, types);
        Map<T, W> map = new HashMap<T, W>(rows.size());
        for (Row row : rows) {
            map.put((T) row.get(keyCol), (W) row.get(valueCol));
        }
        return map;
    }

    public List<Row> query(String sql, Object[] args, int[] types) {
        return query(sql, new ISqlRowMapper<Row>() {
            public Row mapRow(Row row) {
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
    
    public int update(String sql, Object[] values) {
        return update(sql, values, null);
    }

    public int update(String sql) {
        return update(sql, null, null);
    }

    public int update(String... sql) {
        return update(true, true, -1, sql);
    }

    public int update(Table table, Map<String, Object> params) {
        return update(DmlType.UPDATE, table, params);
    }

    public int insert(Table table, Map<String, Object> params) {
        return update(DmlType.INSERT, table, params);
    }

    public int delete(Table table, Map<String, Object> params) {
        return update(DmlType.DELETE, table, params);
    }

    public void save(Table table, Map<String, Object> params) {
        if (update(table, params) == 0) {
            insert(table, params);
        }
    }

    protected int update(DmlType type, Table table, Map<String, Object> params) {
        DmlStatement updateStmt = new DmlStatement(type, table.getCatalog(), table.getSchema(),
                table.getName(), table.getPrimaryKeyColumns(), table.getColumns(),
                dateOverrideToTimestamp, identifierQuoteString);
        String sql = updateStmt.getSql();
        int[] types = updateStmt.getTypes();
        Object[] values = updateStmt.buildArgsFrom(params);
        return update(sql, values, types);
    }

}
