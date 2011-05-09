package org.jumpmind.symmetric.core.sql;

import org.jumpmind.symmetric.core.db.IDbPlatform;

/**
 * This interface insulates the application from the data connection technology.
 */
public interface ISqlConnection {

    public IDbPlatform getDbPlatform();

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args);

    public int queryForInt(String sql);

    public int update(String sql);

    public int update(String... sql);

    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql);

    public int update(String sql, Object[] values, int[] types);

    public <T> ISqlReadCursor<T> query(String sql, ISqlRowMapper<T> mapper,
            Object[] values, int[] types);

    public <T> ISqlReadCursor<T> query(String sql, ISqlRowMapper<T> mapper);

    public void testConnection();

    public DbException translate(Exception ex);

}
