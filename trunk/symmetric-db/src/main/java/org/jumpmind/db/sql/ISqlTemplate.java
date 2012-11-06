package org.jumpmind.db.sql;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * This interface insulates the application from the data connection technology.
 */
public interface ISqlTemplate {

    @Deprecated
    public byte[] queryForBlob(String sql, Object... args);

    public byte[] queryForBlob(String sql, int jdbcTypeCode, String jdbcTypeName, Object... args);

    public String queryForClob(String sql, Object... args);

    public <T> T queryForObject(String sql, Class<T> clazz, Object... params);
    
    public <T> T queryForObject(String sql, ISqlRowMapper<T> mapper, Object... params);
    
    public int queryForInt(String sql, Map<String,Object> params);
    
    public int queryForInt(String sql, Object... args);
    
    public String queryForString(String sql, Object... args);

    public long queryForLong(String sql, Object... args);
    
    public Row queryForRow(String sql, Object... args);

    public Map<String, Object> queryForMap(String sql, Object... params);

    public <T> Map<String, T> queryForMap(String sql, ISqlRowMapper<T> mapper, String keyColumn,
            Object... args);
    
    public <T> Map<String, T> queryForMap(String sql, String keyColumn, String valueColumn,
            Object... args);

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] params, int[] types);

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper);

    public List<Row> query(String sql);

    public List<Row> query(String sql, Object[] params, int[] types);
    
    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Map<String,Object> namedParams);

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object... params);

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Object[] params, int[] types);
    
    public <T> List<T> query(String sql, int maxRowsToFetch, ISqlRowMapper<T> mapper, Object[] params, int[] types);
    
    public <T> List<T> query(String sql, int maxRowsToFetch, ISqlRowMapper<T> mapper, Object... params);
    
    public <T> List<T> query(String sql, int maxRowsToFetch, ISqlRowMapper<T> mapper, Map<String,Object> params);

    public <T, W> Map<T, W> query(String sql, String keyCol, String valueCol, Object[] params,
            int[] types);

    public int update(boolean autoCommit, boolean failOnError, int commitRate, ISqlResultsListener listener, String... sql);
    
    public int update(boolean autoCommit, boolean failOnError, int commitRate, ISqlResultsListener listener, ISqlStatementSource source);
    
    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql);

    public int update(String sql, Object[] values, int[] types);

    public int update(String sql, Object... values);

    public void testConnection();

    public SqlException translate(Throwable ex);
    
    public boolean isUniqueKeyViolation(Throwable ex);
    
    public boolean isForeignKeyViolation(Throwable ex);

    public ISqlTransaction startSqlTransaction();

    public int getDatabaseMajorVersion();

    public int getDatabaseMinorVersion();

    public String getDatabaseProductName();

    public String getDatabaseProductVersion();

    public String getDriverName();

    public String getDriverVersion();

    public Set<String> getSqlKeywords();

    public boolean supportsGetGeneratedKeys();
    
    public boolean isStoresUpperCaseIdentifiers();
    
    public boolean isStoresLowerCaseIdentifiers();
    
    public boolean isStoresMixedCaseQuotedIdentifiers();

    public long insertWithGeneratedKey(final String sql, String column, final String sequenceName,
            final Object[] args, final int[] types);    

}
