/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db.sql;

import static org.jumpmind.db.model.ColumnTypes.ORACLE_TIMESTAMPLTZ;
import static org.jumpmind.db.model.ColumnTypes.ORACLE_TIMESTAMPTZ;

import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.lang.reflect.Method;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Blob;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Arrays;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.exception.IoException;
import org.jumpmind.util.LinkedCaseInsensitiveMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.SqlTypeValue;
import org.springframework.jdbc.core.StatementCreatorUtils;
import org.springframework.jdbc.support.lob.DefaultLobHandler;
import org.springframework.jdbc.support.lob.LobHandler;

public class JdbcSqlTemplate extends AbstractSqlTemplate implements ISqlTemplate {

    static final Logger log = LoggerFactory.getLogger(JdbcSqlTemplate.class);

    protected DataSource dataSource;

    protected boolean requiresAutoCommitFalseToSetFetchSize = false;

    protected SqlTemplateSettings settings;

    protected SymmetricLobHandler lobHandler;

    protected Boolean supportsGetGeneratedKeys = null;

    protected int[] primaryKeyViolationCodes;

    protected String[] primaryKeyViolationSqlStates;
    
    protected String[] primaryKeyViolationMessageParts;

    protected int[] foreignKeyViolationCodes;

    protected String[] foreignKeyViolationSqlStates;
    
    protected String[] foreignKeyViolationMessageParts;

    protected int isolationLevel;

    public JdbcSqlTemplate(DataSource dataSource, SqlTemplateSettings settings,
            SymmetricLobHandler lobHandler, DatabaseInfo databaseInfo) {
        this.dataSource = dataSource;
        this.settings = settings == null ? new SqlTemplateSettings() : settings;
        this.lobHandler = lobHandler == null ? new SymmetricLobHandler(new DefaultLobHandler())
                : lobHandler;
        this.isolationLevel = databaseInfo.getMinIsolationLevelToPreventPhantomReads();
    }

    protected Connection getConnection() throws SQLException {
        return this.dataSource.getConnection();
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public boolean isRequiresAutoCommitFalseToSetFetchSize() {
        return requiresAutoCommitFalseToSetFetchSize;
    }

    public void setSettings(SqlTemplateSettings settings) {
        this.settings = settings;
    }

    public SqlTemplateSettings getSettings() {
        return settings;
    }

    public SymmetricLobHandler getLobHandler() {
        return lobHandler;
    }

    public <T> ISqlReadCursor<T> queryForCursor(String sql, ISqlRowMapper<T> mapper, Object[] args,
            int[] types) {
        logSql(sql, args);
        return new JdbcSqlReadCursor<T>(this, mapper, sql, args, types);
    }

    public int getIsolationLevel() {
        return isolationLevel;
    }

    public void setIsolationLevel(int isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public <T> T queryForObject(final String sql, final Class<T> clazz, final Object... args) {
        logSql(sql, args);
        return execute(new IConnectionCallback<T>() {
            public T execute(Connection con) throws SQLException {
                T result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(expandSql(sql, args));
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    setValues(ps, expandArgs(sql, args));
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = getObjectFromResultSet(rs, clazz);
                    }
                } finally {
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }

    @Deprecated
    public byte[] queryForBlob(final String sql, final Object... args) {
        return queryForBlob(sql, -1, null, args);
    }

    public byte[] queryForBlob(final String sql, final int jdbcTypeCode, final String jdbcTypeName,
            final Object... args) {
        logSql(sql, args);
        return execute(new IConnectionCallback<byte[]>() {
            public byte[] execute(Connection con) throws SQLException {
                if (lobHandler.needsAutoCommitFalseForBlob(jdbcTypeCode, jdbcTypeName)) {
                    con.setAutoCommit(false);
                }
                byte[] result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = lobHandler.getBlobAsBytes(rs, 1, jdbcTypeCode, jdbcTypeName);
                    }
                } finally {
                    if (lobHandler.needsAutoCommitFalseForBlob(jdbcTypeCode, jdbcTypeName)
                            && con != null) {
                        con.setAutoCommit(true);
                    }
                    close(rs);
                    close(ps);
                }
                return result;
            }
        });
    }
    
    @Deprecated
    public String queryForClob(final String sql, final Object... args) {
        return queryForClob(sql, -1, null, args);
    }

    public String queryForClob(final String sql, final int jdbcTypeCode, final String jdbcTypeName, final Object... args) {
        logSql(sql, args);
        return execute(new IConnectionCallback<String>() {
            public String execute(Connection con) throws SQLException {
                String result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = lobHandler.getClobAsString(rs, 1, jdbcTypeCode, jdbcTypeName);
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
        logSql(sql, args);
        return execute(new IConnectionCallback<Map<String, Object>>() {
            public Map<String, Object> execute(Connection con) throws SQLException {
                Map<String, Object> result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    ps.setQueryTimeout(settings.getQueryTimeout());
                    if (args != null && args.length > 0) {
                        setValues(ps, args);
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

    public ISqlTransaction startSqlTransaction(boolean autoCommit) {
        return new JdbcSqlTransaction(this, autoCommit);
    }

    public ISqlTransaction startSqlTransaction() {
        return new JdbcSqlTransaction(this);
    }

    public int update(final String sql, final Object[] args, final int[] types) {
        logSql(sql, args);
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                if (args == null) {
                    Statement stmt = null;
                    try {
                        stmt = con.createStatement();
                        stmt.setQueryTimeout(settings.getQueryTimeout());
                        stmt.execute(sql);
                        return stmt.getUpdateCount();
                    } finally {
                        close(stmt);
                    }
                } else {
                    PreparedStatement ps = null;
                    try {
                        ps = con.prepareStatement(sql);
                        ps.setQueryTimeout(settings.getQueryTimeout());
                        if (types != null) {
                            setValues(ps, args, types, getLobHandler()
                                    .getDefaultHandler());
                        } else {
                            setValues(ps, args);
                        }
                        ps.execute();
                        return ps.getUpdateCount();
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

    public int update(boolean autoCommit, boolean failOnError, int commitRate,
            ISqlResultsListener resultsListener, String... sql) {
        return this.update(autoCommit, failOnError, true, true,
                commitRate, resultsListener, new ListSqlStatementSource(sql));
    }

    public int update(final boolean autoCommit, final boolean failOnError, final boolean failOnDrops,
            final boolean failOnSequenceCreate, final int commitRate, final ISqlResultsListener resultsListener, final ISqlStatementSource source) {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                int totalUpdateCount = 0;
                boolean oldAutoCommitSetting = con.getAutoCommit();
                Statement stmt = null;
                try {
                    con.setAutoCommit(autoCommit);
                    stmt = con.createStatement();
                    int statementCount = 0;
                    for (String statement = source.readSqlStatement(); statement != null; statement = source
                            .readSqlStatement()) {
                        logSql(statement, null);
                        try {
                            boolean hasResults = stmt.execute(statement);
                            int updateCount = stmt.getUpdateCount();
                            totalUpdateCount += updateCount;
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
                            boolean isDrop = statement.toLowerCase().trim().startsWith("drop");
                            boolean isSequenceCreate = statement.toLowerCase().trim().startsWith("create sequence");
                            if (resultsListener != null) {
                                resultsListener.sqlErrored(statement, translate(statement, ex),
                                        statementCount, isDrop, isSequenceCreate);
                            }

                            if ((isDrop && !failOnDrops) || (isSequenceCreate && !failOnSequenceCreate)) {
                                log.debug("{}.  Failed to execute: {}", ex.getMessage(), statement);
                            } else {
                                log.warn("{}.  Failed to execute: {}", ex.getMessage(), statement);
                                if (failOnError) {
                                    throw ex;
                                }
                            }

                        }
                    }

                    if (!autoCommit) {
                        con.commit();
                    }
                    return totalUpdateCount;
                } catch (SQLException ex) {
                    if (!autoCommit) {
                        con.rollback();
                    }
                    throw ex;
                } finally {
                    close(stmt);
                    if (!con.isClosed()) {
                       con.setAutoCommit(oldAutoCommitSetting);
                    }
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
            c = getConnection();
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
     * @param readStringsAsBytes TODO
     * @return the value object
     * @throws SQLException
     *             if thrown by the JDBC API
     * @see java.sql.Blob
     * @see java.sql.Clob
     * @see java.sql.Timestamp
     */
    public static Object getResultSetValue(ResultSet rs, int index, boolean readStringsAsBytes) throws SQLException {
        ResultSetMetaData metaData = rs.getMetaData();
        Object obj = null;
        int jdbcType = metaData.getColumnType(index);
        if (readStringsAsBytes && TypeMap.isTextType(jdbcType)) {
            byte[] bytes = rs.getBytes(index);
            if (bytes != null) {
                obj = new String(bytes);
            }
        } else {
            obj = rs.getObject(index);
        }
        String className = null;
        if (obj != null) {
            className = obj.getClass().getName();
        }
        if (obj instanceof Blob) {
            Blob blob = (Blob) obj;
            InputStream is = blob.getBinaryStream();
            try {
                obj = IOUtils.toByteArray(is);
            } catch (IOException e) {
                throw new SqlException(e);
            } finally {
                IOUtils.closeQuietly(is);
            }
        } else if (obj instanceof Clob) {
            Clob clob = (Clob) obj;
            Reader reader = clob.getCharacterStream();
            try {
                obj = IOUtils.toString(reader);
            } catch (IOException e) {
                throw new SqlException(e);
            } finally {
                IOUtils.closeQuietly(reader);
            }
        } else if (className != null && ("oracle.sql.TIMESTAMP".equals(className))) {
            obj = rs.getTimestamp(index);
        } else if (className != null && "oracle.sql.TIMESTAMPTZ".equals(className)) {
            obj = rs.getString(index);
        } else if (className != null && "oracle.sql.TIMESTAMPLTZ".equals(className)) {
            obj = rs.getString(index);            
        } else if (className != null && className.startsWith("oracle.sql.DATE")) {
            String metaDataClassName = metaData.getColumnClassName(index);
            if ("java.sql.Timestamp".equals(metaDataClassName)
                    || "oracle.sql.TIMESTAMP".equals(metaDataClassName)) {
                obj = rs.getTimestamp(index);
            } else {
                obj = rs.getDate(index);
            }
        } else if (obj instanceof java.sql.Date) {
            String metaDataClassName = metaData.getColumnClassName(index);
            if ("java.sql.Timestamp".equals(metaDataClassName)) {
                obj = rs.getTimestamp(index);
            }
        } else if (obj instanceof Timestamp) {
            String typeName = metaData.getColumnTypeName(index);
            if (typeName != null && typeName.equals("timestamptz")) {
                obj = rs.getString(index);
            }
        }
        return obj;
    }

    public static void close(ResultSet rs) {
        try {
            if (rs != null) {
                rs.close();
            }
        } catch (Throwable ex) {
        }
    }

    public static void close(PreparedStatement ps) {
        try {
            if (ps != null) {
                ps.close();
            }
        } catch (Throwable ex) {
        }
    }

    public static void close(Statement stmt) {
        try {
            if (stmt != null) {
                stmt.close();
            }
        } catch (Throwable ex) {
        }
    }

    public static void close(boolean autoCommitValue, Connection c) {
        try {
            if (c != null) {
                c.setAutoCommit(autoCommitValue);
            }
        } catch (Throwable ex) {
        } finally {
            close(c);
        }
    }

    public static void close(boolean autoCommitValue, int transactionIsolationLevel, Connection c) {
        try {
            if (c != null) {
                c.setAutoCommit(autoCommitValue);
                if (c.getTransactionIsolation() != transactionIsolationLevel) {
                    c.setTransactionIsolation(transactionIsolationLevel);
                }
            }
        } catch (Throwable ex) {
        } finally {
            close(c);
        }
    }

    public static void close(Connection c) {
        try {
            if (c != null) {
                c.close();
            }
        } catch (Throwable ex) {
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
                String replaceSql = sql.replaceFirst("\\([\"|\\w]*,", "(").replaceFirst("\\(null,",
                        "(");
                if (supportsGetGeneratedKeys) {
                    ps = conn.prepareStatement(replaceSql, Statement.RETURN_GENERATED_KEYS);
                } else {
                    ps = conn.prepareStatement(replaceSql);
                }
            }
            ps.setQueryTimeout(settings.getQueryTimeout());
            setValues(ps, args, types, lobHandler.getDefaultHandler());

            ResultSet rs = null;
            if (supportsGetGeneratedKeys) {
                ps.execute();
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
                ps.execute();
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

    public boolean isUniqueKeyViolation(Throwable ex) {
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
                
                if (primaryKeyViolationMessageParts != null) {
                	String sqlMessage = sqlEx.getMessage();
                	if (sqlMessage != null) {
                		sqlMessage = sqlMessage.toLowerCase();
                		for (String primaryKeyViolationMessagePart : primaryKeyViolationMessageParts) {
                			if (primaryKeyViolationMessagePart != null && sqlMessage.contains(primaryKeyViolationMessagePart.toLowerCase())) {
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

    public boolean isForeignKeyViolation(Throwable ex) {
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
                
                if (foreignKeyViolationMessageParts != null) {
                	String sqlMessage = sqlEx.getMessage();
                	if (sqlMessage != null) {
                		sqlMessage = sqlMessage.toLowerCase();
                		for (String foreignKeyViolationMessagePart : foreignKeyViolationMessageParts) {
                			if (foreignKeyViolationMessagePart != null && sqlMessage.contains(foreignKeyViolationMessagePart.toLowerCase())) {
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

    @SuppressWarnings("unchecked")
    public <T> T getObjectFromResultSet(ResultSet rs, Class<T> clazz) throws SQLException {
        T result;
        if (Date.class.isAssignableFrom(clazz)) {
            result = (T) rs.getTimestamp(1);
        } else if (String.class.isAssignableFrom(clazz)) {
            result = (T) rs.getString(1);
        } else if (Long.class.isAssignableFrom(clazz)) {
            result = (T)new Long(rs.getLong(1));
        } else if (Integer.class.isAssignableFrom(clazz)) {
            result = (T)new Integer(rs.getInt(1));
        } else if (Float.class.isAssignableFrom(clazz)) {
            result = (T)new Float(rs.getFloat(1));
        } else if (Double.class.isAssignableFrom(clazz)) {
            result = (T)new Double(rs.getDouble(1));
        } else if (BigDecimal.class.isAssignableFrom(clazz)) {
            result = (T)rs.getBigDecimal(1);
        } else {
            result = (T) rs.getObject(1);
        }
        return result;
    }

    public void setValues(PreparedStatement ps, Object[] args, int[] argTypes,
            LobHandler lobHandler) throws SQLException {
        for (int i = 1; i <= args.length; i++) {
            Object arg = args[i - 1];
            int argType = argTypes != null && argTypes.length >= i ? argTypes[i - 1] : SqlTypeValue.TYPE_UNKNOWN;
            if (argType == Types.BLOB && lobHandler != null && arg instanceof byte[]) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, (byte[]) arg);
            } else if (argType == Types.BLOB && lobHandler != null && arg instanceof String) {
                lobHandler.getLobCreator().setBlobAsBytes(ps, i, arg.toString().getBytes());
            } else if (argType == Types.CLOB && lobHandler != null) {
                lobHandler.getLobCreator().setClobAsString(ps, i, (String) arg);
            } else if ((argType == Types.DECIMAL || argType == Types.NUMERIC) && arg != null) {
                setDecimalValue(ps, i, arg, argType);
            } else {
                StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(arg, argType), arg);
            }
        }
    }

    protected void setDecimalValue(PreparedStatement ps, int i, Object arg, int argType) throws SQLException {
        StatementCreatorUtils.setParameterValue(ps, i, verifyArgType(arg, argType), arg);
    }

    protected int verifyArgType(Object arg, int argType) {
        if (argType == ORACLE_TIMESTAMPTZ || argType == ORACLE_TIMESTAMPLTZ || argType == Types.OTHER) {
            return SqlTypeValue.TYPE_UNKNOWN;
        } else if ((argType == Types.INTEGER && arg instanceof BigInteger) ||
                (argType == Types.BIGINT && arg instanceof BigDecimal)) {
            return Types.DECIMAL;
        } else {
            return argType;
        }
    }

    public void setValues(PreparedStatement ps, Object[] args) throws SQLException {
        if (args != null) {
            for (int i = 0; i < args.length; i++) {
                Object arg = args[i];
                doSetValue(ps, i + 1, arg);
            }
        }
    }

    /**
     * Set the value for prepared statements specified parameter index using the
     * passed in value. This method can be overridden by sub-classes if needed.
     *
     * @param ps
     *            the PreparedStatement
     * @param parameterPosition
     *            index of the parameter position
     * @param argValue
     *            the value to set
     * @throws SQLException
     */
    public void doSetValue(PreparedStatement ps, int parameterPosition, Object argValue)
            throws SQLException {
        StatementCreatorUtils.setParameterValue(ps, parameterPosition, SqlTypeValue.TYPE_UNKNOWN, argValue);
    }

}
