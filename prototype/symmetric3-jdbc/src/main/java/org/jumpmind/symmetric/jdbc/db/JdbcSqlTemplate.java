package org.jumpmind.symmetric.jdbc.db;

import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.db.AbstractSqlTemplate;
import org.jumpmind.symmetric.core.db.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlReadCursor;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.model.Parameters;

// TODO make sure connection timeouts are set properly
public class JdbcSqlTemplate extends AbstractSqlTemplate implements ISqlTemplate {

    static final Log log = LogFactory.getLog(JdbcSqlTemplate.class);

    protected IJdbcDbDialect dbDialect;

    public JdbcSqlTemplate(DataSource dataSource) {
        this.dbDialect = JdbcDbDialectFactory.createPlatform(dataSource, new Parameters());
    }

    public JdbcSqlTemplate(IJdbcDbDialect platform) {
        this.dbDialect = platform;
    }

    public IDbDialect getDbDialect() {
        return this.dbDialect;
    }

    public IJdbcDbDialect getJdbcDbPlatform() {
        return this.dbDialect;
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper,
            Object[] values, int[] types) {
        return new JdbcSqlReadCursor<T>(sql, values, types, mapper, this.dbDialect);
    }

    public <T> T queryForObject(final String sql, Class<T> clazz, final Object... args) {
        return execute(new IConnectionCallback<T>() {
            @SuppressWarnings("unchecked")
            public T execute(Connection con) throws SQLException {
                T result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    StatementCreatorUtil.setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = (T) rs.getObject(1);
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
        return new JdbcSqlTransaction(dbDialect);
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
                        StatementCreatorUtil.setValues(ps, values, types,
                                ((IJdbcDbDialect) getDbDialect()).getLobHandler());
                        return ps.executeUpdate();
                    } finally {
                        close(ps);
                    }
                }
            }
        });
    }

    public int update(final boolean autoCommit, final boolean failOnError, final int commitRate,
            final String... sql) {
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
                            updateCount += stmt.executeUpdate(statement);
                            statementCount++;
                            if (statementCount % commitRate == 0 && !autoCommit) {
                                con.commit();
                            }
                        } catch (SQLException ex) {
                            if (!failOnError) {
                                log.log(LogLevel.WARN, "%s.  Failed to execute: %s.",
                                        ex.getMessage(), sql);
                            } else {
                                throw translate(statement, ex);
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
            c = this.dbDialect.getDataSource().getConnection();
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

    public SqlException translate(Exception ex) {
        return translate(ex.getMessage(), ex);
    }

    public SqlException translate(String message, Exception ex) {
        if (getDbDialect().isDataIntegrityException(ex)) {
            return new DataIntegrityViolationException(message, ex);
        } else if (ex instanceof SqlException) {
            return (SqlException) ex;
        } else {
            return new SqlException(message, ex);
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

}
