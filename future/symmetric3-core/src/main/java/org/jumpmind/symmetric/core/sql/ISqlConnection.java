package org.jumpmind.symmetric.core.sql;

import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.db.IPlatform;

/**
 * This interface insulates the application from the data connection technology.
 */
public interface ISqlConnection {
    
    public IPlatform getPlatform();

    public <T> T queryForObject(String sql, Class<T> clazz, Object... args);
    
    public int queryForInt(String sql);
    
    public int update(String sql);
    
    public int update(String... sql);
    
    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql);
    
    public int update(String sql, Object[] values, int[] types);
    
    public void testConnection();
    
    public DbException translate(Exception ex);
    
}
