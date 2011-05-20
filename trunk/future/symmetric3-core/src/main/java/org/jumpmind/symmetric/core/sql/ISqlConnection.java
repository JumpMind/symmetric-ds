package org.jumpmind.symmetric.core.sql;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.db.IDbPlatform;

/**
 * This interface insulates the application from the data connection technology.
 */
public interface ISqlConnection {

    public IDbPlatform getDbPlatform();

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args);

    public int queryForInt(String sql);

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] values, int[] types);

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper);

    public List<Map<String, Object>> query(String sql);

    public List<Map<String, Object>> query(String sql, Object[] args, int[] types);

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper);

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object[] args, int[] types);

    public int update(String sql);

    public int update(String... sql);

    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql);

    public int update(String sql, Object[] values, int[] types);

    public void testConnection();

    public SqlException translate(Exception ex);

    public ISqlTransaction startSqlTransaction();

}
