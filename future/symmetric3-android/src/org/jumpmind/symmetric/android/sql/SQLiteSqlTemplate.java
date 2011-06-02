package org.jumpmind.symmetric.android.sql;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlReadCursor;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.core.db.SqlException;

public class SQLiteSqlTemplate implements ISqlTemplate {

    public IDbDialect getDbDialect() {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args) {
        // TODO Auto-generated method stub
        return null;
    }

    public int queryForInt(String sql) {
        // TODO Auto-generated method stub
        return 0;
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] values, int[] types) {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Map<String, Object>> query(String sql) {
        // TODO Auto-generated method stub
        return null;
    }

    public List<Map<String, Object>> query(String sql, Object[] args, int[] types) {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper) {
        // TODO Auto-generated method stub
        return null;
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object[] args, int[] types) {
        // TODO Auto-generated method stub
        return null;
    }

    public int update(String sql) {
        // TODO Auto-generated method stub
        return 0;
    }

    public int update(String... sql) {
        // TODO Auto-generated method stub
        return 0;
    }

    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql) {
        // TODO Auto-generated method stub
        return 0;
    }

    public int update(String sql, Object[] values, int[] types) {
        // TODO Auto-generated method stub
        return 0;
    }

    public void testConnection() {
        // TODO Auto-generated method stub

    }

    public SqlException translate(Exception ex) {
        // TODO Auto-generated method stub
        return null;
    }

    public ISqlTransaction startSqlTransaction() {
        // TODO Auto-generated method stub
        return null;
    }

}
