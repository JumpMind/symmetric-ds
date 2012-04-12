package org.jumpmind.db.sql;

import java.io.IOException;
import java.lang.reflect.Method;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.exception.IoException;
import org.jumpmind.util.LinkedCaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

public class JdbcSqlTemplate extends AbstractSqlTemplate implements ISqlTemplate {

    static final Logger log = LoggerFactory.getLogger(JdbcSqlTemplate.class);

    protected DataSource dataSource;

    protected boolean requiresAutoCommitFalseToSetFetchSize = false;

    protected DatabasePlatformSettings settings;

    protected LobHandler lobHandler;

    protected Boolean supportsGetGeneratedKeys = null;

    protected int[] primaryKeyViolationCodes;

    protected String[] primaryKeyViolationSqlStates;
    
    protected int[] foreignKeyViolationCodes;

    protected String[] foreignKeyViolationSqlStates;

    public JdbcSqlTemplate(DataSource dataSource, DatabasePlatformSettings settings,
            LobHandler lobHandler) {
        this.dataSource = dataSource;
        this.settings = settings;
        this.lobHandler = lobHandler == null ? new DefaultLobHandler() : lobHandler;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isRequiresAutoCommitFalseToSetFetchSize() {
        return requiresAutoCommitFalseToSetFetchSize;
    }

    public void setSettings(DatabasePlatformSettings settings) {
        this.settings = settings;
    }

    public DatabasePlatformSettings getSettings() {
        return settings;
    }

    public LobHandler getLobHandler() {
        return lobHandler;
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] values, int[] types) {
        return new JdbcSqlReadCursor<T>(this, mapper, sql, values, types);
    }

    public <T> T queryForObject(final String sql, final Class<T> clazz, final Object... args) {
        return execute(new IConnectionCallback<T>() {
            public T execute(Connection con) throws SQLException {
                T result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(expandSql(sql, args));
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    JdbcUtils.setValues(ps, expandArgs(sql, args));
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = JdbcUtils.getObjectFromResultSet(rs, clazz);
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    public byte[] queryForBlob(final String sql, final Object... args) {
        return execute(new IConnectionCallback<byte[]>() {
            public byte[] execute(Connection con) throws SQLException {
                byte[] result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    JdbcUtils.setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = lobHandler.getBlobAsBytes(rs, 1);
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    public String queryForClob(final String sql, final Object... args) {
        return execute(new IConnectionCallback<String>() {
            public String execute(Connection con) throws SQLException {
                String result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    JdbcUtils.setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = lobHandler.getClobAsString(rs, 1);
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    public Map<String, Object> queryForMap(final String sql, final Object... args) {
        return execute(new IConnectionCallback<Map<String, Object>>() {
            public Map<String, Object> execute(Connection con) throws SQLException {
                Map<String, Object> result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    if (args != null && args.length > 0) {
                        JdbcUtils.setValues(ps, args);
                    }
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        ResultSetMetaData meta = rs.getMetaData();
                        int colCount = meta.getColumnCount();
                        result = new LinkedCaseInsensitiveMap<Object>(colCount);
                        for (int i = 1; i <= colCount; i++) {
                            String key = meta.getColumnName(i);
                            Object value = rs.getObject(i);
                            if (value instanceof Blob) {
                                Blob blob = (Blob) value;
                                try {
                                    value = IOUtils.toByteArray(blob.getBinaryStream());
                                } catch (IOException e) {
                                    throw new IoException(e);
                                }
                            } else if (value instanceof Clob) {
                                Clob clob = (Clob) value;
                                try {
                                    value = IOUtils.toByteArray(clob.getCharacterStream());
                                } catch (IOException e) {
                                    throw new IoException(e);
                                }
                            } else if (value != null) {
                                Class<?> clazz = value.getClass();
                                Class<?> superClazz = clazz.getSuperclass();
                                if (superClazz != null
                                        && superClazz.getName().equals("oracle.sql.Datum")) {
                                    try {
                                        Method method = superClazz.getMethod("toJdbc");
                                        value = method.invoke(value);
                                    } catch (Exception e) {
                                        throw new IllegalStateException(e);
                                    }
                                }
                            }
                            result.put(key, value);
                        }
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    public ISqlTransaction startSqlTransaction() {
        return new JdbcSqlTransaction(this);
    }

    public int update(final String sql, final Object[] values, final int[] types) {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                if (values == null) {
                    Statement stmt = null;
                    try {
                        stmt = con.createStatement();
                        return stmt.executeUpdate(sql);
                    } finally {
                        close(stmt);
                    }
                } else {
                    PreparedStatement ps = null;
                    try {
                        ps = con.prepareStatement(sql);
                        ps.setQueryTimeout(settings.getQueryTimeout());
                        if (types != null) {
                            JdbcUtils.setValues(ps, values, types, getLobHandler());
                        } else {
                            JdbcUtils.setValues(ps, values);
                        }
                        return ps.executeUpdate();
                    } finally {
                        close(ps);
                    }
                }
            }
        });
    }

    public int update(boolean autoCommit, boolean failOnError, int commitRate, String... sql) {
        return update(autoCommit, failOnError, commitRate, null, sql);
    }

    public int update(final boolean autoCommit, final boolean failOnError, final int commitRate,
            final ISqlResultsListener resultsListener, final String... sql) {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                int updateCount = 0;
                boolean oldAutoCommitSetting = con.getAutoCommit();
                Statement stmt = null;
                try {
                    con.setAutoCommit(autoCommit);
                    stmt = con.createStatement();
                    int statementCount = 0;
                    for (String statement : sql) {
                        try {
                            boolean hasResults = stmt.execute(statement);
                            updateCount += stmt.getUpdateCount();
                            int rowsRetrieved = 0;
                            if (hasResults) {
                                ResultSet rs = null;
                                try {
                                    rs = stmt.getResultSet();
                                    while (rs.next()) {
                                        rowsRetrieved++;
                                    }
                                } finally {
                                    close(rs);
                                }
                            }
                            if (resultsListener != null) {
                                resultsListener.sqlApplied(statement, updateCount, rowsRetrieved,
                                        statementCount);
                            }
                            statementCount++;
                            if (statementCount % commitRate == 0 && !autoCommit) {
                                con.commit();
                            }
                        } catch (SQLException ex) {
                            if (resultsListener != null) {
                                resultsListener.sqlErrored(statement, translate(statement, ex),
                                        statementCount);
                            }
                            if (!failOnError) {
                                if (statement.toLowerCase().startsWith("drop")) {
                                    log.debug("{}.  Failed to execute: {}.", ex.getMessage(),
                                            statement);
                                } else {
                                    log.warn("{}.  Failed to execute: {}.", ex.getMessage(),
                                            statement);
                                }
                            } else {
                                throw ex;
                            }
                        }
                    }

                    if (!autoCommit) {
                        con.commit();
                    }
                    return updateCount;
                } catch (SQLException ex) {
                    if (!autoCommit) {
                        con.rollback();
                    }
                    throw ex;
                } finally {
                    close(stmt);
                    con.setAutoCommit(oldAutoCommitSetting);
                }
            }
        });
    }

    public void testConnection() {
        execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                return true;
            }
        });
    }

    public <T> T execute(IConnectionCallback<T> callback) {
        Connection c = null;
        try {
            c = dataSource.getConnection();
            return callback.execute(c);
        } catch (SQLException ex) {
            throw translate(ex);
        } finally {
            close(c);
        }
    }

    /**
     * Determine the column name to use. The column name is determined based on
     * a lookup using ResultSetMetaData.
     * <p>
     * This method implementation takes into account recent clarifications
     * expressed in the JDBC 4.0 specification:
     * <p>
     * <i>columnLabel - the label for the column specified with the SQL AS
     * clause. If the SQL AS clause was not specified, then the label is the
     * name of the column</i>.
     * 
     * @return the column name to use
     * @param resultSetMetaData
     *            the current meta data to use
     * @param columnIndex
     *            the index of the column for the look up
     * @throws SQLException
     *             in case of lookup failure
     */
    public static String lookupColumnName(ResultSetMetaData resultSetMetaData, int columnIndex)
            throws SQLException {
        String name = resultSetMetaData.getColumnLabel(columnIndex);
        if (name == null || name.length() < 1) {
            name = resultSetMetaData.getColumnName(columnIndex);
        }
        return name;
    }

    /**
     * Retrieve a JDBC column value from a ResultSet, using the most appropriate
     * value type. The returned value should be a detached value object, not
     * having any ties to the active ResultSet: in particular, it should not be
     * a Blob or Clob object but rather a byte array respectively String
     * representation.
     * <p>
     * Uses the <code>getObject(index)</code> method, but includes additional
     * "hacks" to get around Oracle 10g returning a non-standard object for its
     * TIMESTAMP datatype and a <code>java.sql.Date</code> for DATE columns
     * leaving out the time portion: These columns will explicitly be extracted
     * as standard <code>java.sql.Timestamp</code> object.
     * 
     * @param rs
     *            is the ResultSet holding the data
     * @param index
     *            is the column index
     * @return the value object
     * @throws SQLException
     *             if thrown by the JDBC API
     * @see java.sql.Blob
     * @see java.sql.Clob
     * @see java.sql.Timestamp
     */
    public static Object getResultSetValue(ResultSet rs, int index) throws SQLException {
        Object obj = rs.getObject(index);
        String className = null;
        if (obj != null) {
            className = obj.getClass().getName();
        }
        if (obj instanceof Blob) {
            obj = rs.getBytes(index);
        } else if (obj instanceof Clob) {
            obj = rs.getString(index);
        } else if (className != null
                && ("oracle.sql.TIMESTAMP".equals(className) || "oracle.sql.TIMESTAMPTZ"
                        .equals(className))) {
            obj = rs.getTimestamp(index);
        } else if (className != null && className.startsWith("oracle.sql.DATE")) {
            String metaDataClassName = rs.getMetaData().getColumnClassName(index);
            if ("java.sql.Timestamp".equals(metaDataClassName)
                    || "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
                obj = rs.getTimestamp(index);
            } else {
                obj = rs.getDate(index);
            }
        } else if (obj != null && obj instanceof java.sql.Date) {
            if ("java.sql.Timestamp".equals(rs.getMetaData().getColumnClassName(index))) {
                obj = rs.getTimestamp(index);
            }
        }
        return obj;
    }

    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (SQLException ex) {
        }
    }

    public static void close(boolean autoCommitValue, Connection c) {
        try {
            if (c != null) {
                c.setAutoCommit(autoCommitValue);
            }
        } catch (SQLException ex) {
        } finally {
            close(c);
        }
    }

    public static void close(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (SQLException ex) {
        }
    }

    public int getDatabaseMajorVersion() {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                return con.getMetaData().getDatabaseMajorVersion();
            }
        });
    }

    public int getDatabaseMinorVersion() {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                return con.getMetaData().getDatabaseMinorVersion();
            }
        });
    }

    public String getDatabaseProductName() {
        return execute(new IConnectionCallback<String>() {
            public String execute(Connection con) throws SQLException {
                return con.getMetaData().getDatabaseProductName();
            }
        });
    }

    public boolean isStoresMixedCaseQuotedIdentifiers() {
        return execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                return con.getMetaData().storesMixedCaseQuotedIdentifiers();
            }
        });
    }

    public boolean isStoresUpperCaseIdentifiers() {
        return execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                return con.getMetaData().storesUpperCaseIdentifiers();
            }
        });
    }
    
    public boolean isStoresLowerCaseIdentifiers() {
        return execute(new IConnectionCallback<Boolean>() {
            public Boolean execute(Connection con) throws SQLException {
                return con.getMetaData().storesLowerCaseIdentifiers();
            }
        });
    }

    public String getDatabaseProductVersion() {
        return execute(new IConnectionCallback<String>() {
            public String execute(Connection con) throws SQLException {
                return con.getMetaData().getDatabaseProductVersion();
            }
        });
    }

    public String getDriverName() {
        return execute(new IConnectionCallback<String>() {
            public String execute(Connection con) throws SQLException {
                return con.getMetaData().getDriverName();
            }
        });
    }

    public String getDriverVersion() {
        return execute(new IConnectionCallback<String>() {
            public String execute(Connection con) throws SQLException {
                return con.getMetaData().getDriverVersion();
            }
        });
    }

    public Set<String> getSqlKeywords() {
        return execute(new IConnectionCallback<Set<String>>() {
            public Set<String> execute(Connection con) throws SQLException {
                DatabaseMetaData sqlTemplateData = con.getMetaData();
                return new HashSet<String>(Arrays.asList(sqlTemplateData.getSQLKeywords()
                        .split(",")));
            }
        });
    }

    public boolean supportsGetGeneratedKeys() {
        if (supportsGetGeneratedKeys == null) {
            supportsGetGeneratedKeys = execute(new IConnectionCallback<Boolean>() {
                public Boolean execute(Connection con) throws SQLException {
                    return con.getMetaData().supportsGetGeneratedKeys();
                }
            });
        }
        return supportsGetGeneratedKeys;
    }

    public boolean supportsReturningKeys() {
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        return true;
    }

    protected String getSelectLastInsertIdSql(String sequenceName) {
        throw new UnsupportedOperationException();
    }

    public long insertWithGeneratedKey(final String sql, final String column,
            final String sequenceName, final Object[] args, final int[] types) {
        return execute(new IConnectionCallback<Long>() {
            public Long execute(Connection conn) throws SQLException {
                return insertWithGeneratedKey(conn, sql, column, sequenceName, args, types);
            }
        });
    }

    protected long insertWithGeneratedKey(Connection conn, String sql, String column,
            String sequenceName, Object[] args, int[] types) throws SQLException {
        long key = 0;
        PreparedStatement ps = null;
        try {
            boolean supportsGetGeneratedKeys = supportsGetGeneratedKeys();
            boolean supportsReturningKeys = supportsReturningKeys();
            if (allowsNullForIdentityColumn()) {
                if (supportsGetGeneratedKeys) {
                    ps = conn.prepareStatement(sql, new int[] { 1 });
                } else if (supportsReturningKeys) {
                    ps = conn.prepareStatement(sql + " returning " + column);
                } else {
                    ps = conn.prepareStatement(sql);
                }
            } else {
                String replaceSql = sql.replaceFirst("\\(\\w*,", "(").replaceFirst("\\(null,", "(");
                if (supportsGetGeneratedKeys) {
                    ps = conn.prepareStatement(replaceSql, Statement.RETURN_GENERATED_KEYS);
                } else {
                    ps = conn.prepareStatement(replaceSql);
                }
            }
            ps.setQueryTimeout(settings.getQueryTimeout());
            JdbcUtils.setValues(ps, args, types, lobHandler);

            ResultSet rs = null;
            if (supportsGetGeneratedKeys) {
                ps.executeUpdate();
                try {
                    rs = ps.getGeneratedKeys();
                    if (rs.next()) {
                        key = rs.getLong(1);
                    }
                } finally {
                    close(rs);
                }
            } else if (supportsReturningKeys) {
                try {
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        key = rs.getLong(1);
                    }
                } finally {
                    close(rs);
                }
            } else {
                Statement st = null;
                ps.executeUpdate();
                try {
                    st = conn.createStatement();
                    rs = st.executeQuery(getSelectLastInsertIdSql(sequenceName));
                    if (rs.next()) {
                        key = rs.getLong(1);
                    }
                } finally {
                    close(rs);
                    close(st);
                }
            }
        } finally {
            close(ps);
        }
        return key;
    }

    public boolean isUniqueKeyViolation(Exception ex) {
        boolean primaryKeyViolation = false;
        if (primaryKeyViolationCodes != null || primaryKeyViolationSqlStates != null) {
            SQLException sqlEx = findSQLException(ex);
            if (sqlEx != null) {
                if (primaryKeyViolationCodes != null) {
                    int errorCode = sqlEx.getErrorCode();
                    for (int primaryKeyViolationCode : primaryKeyViolationCodes) {
                        if (primaryKeyViolationCode == errorCode) {
                            primaryKeyViolation = true;
                            break;
                        }
                    }
                }

                if (primaryKeyViolationSqlStates != null) {
                    String sqlState = sqlEx.getSQLState();
                    if (sqlState != null) {
                        for (String primaryKeyViolationSqlState : primaryKeyViolationSqlStates) {
                            if (primaryKeyViolationSqlState != null
                                    && primaryKeyViolationSqlState.equals(sqlState)) {
                                primaryKeyViolation = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        return primaryKeyViolation;
    }
    
    public boolean isForeignKeyViolation(Exception ex) {
        boolean foreignKeyViolation = false;
        if (foreignKeyViolationCodes != null || foreignKeyViolationSqlStates != null) {
            SQLException sqlEx = findSQLException(ex);
            if (sqlEx != null) {
                if (foreignKeyViolationCodes != null) {
                    int errorCode = sqlEx.getErrorCode();
                    for (int foreignKeyViolationCode : foreignKeyViolationCodes) {
                        if (foreignKeyViolationCode == errorCode) {
                            foreignKeyViolation = true;
                            break;
                        }
                    }
                }

                if (foreignKeyViolationSqlStates != null) {
                    String sqlState = sqlEx.getSQLState();
                    if (sqlState != null) {
                        for (String foreignKeyViolationSqlState : foreignKeyViolationSqlStates) {
                            if (foreignKeyViolationSqlState != null
                                    && foreignKeyViolationSqlState.equals(sqlState)) {
                                foreignKeyViolation = true;
                                break;
                            }
                        }
                    }
                }
            }
        }

        return foreignKeyViolation;
    }


    protected SQLException findSQLException(Throwable ex) {
        if (ex instanceof SQLException) {
            return (SQLException) ex;
        } else {
            Throwable cause = ex.getCause();
            if (cause != null && !cause.equals(ex)) {
                return findSQLException(cause);
            }
        }
        return null;
    }
}
