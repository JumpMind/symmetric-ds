package org.jumpmind.db.sql;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.jumpmind.db.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TODO Support Oracle's non-standard way of batching
 */
public class JdbcSqlTransaction implements ISqlTransaction {

    protected final static Logger log = LoggerFactory.getLogger(JdbcSqlTransaction.class);

    protected boolean inBatchMode = false;

    protected Connection connection;

    protected String psql;

    protected PreparedStatement pstmt;

    protected JdbcSqlTemplate jdbcSqlTemplate;

    protected boolean oldAutoCommitValue;

    protected List<Object> markers = new ArrayList<Object>();

    public JdbcSqlTransaction(JdbcSqlTemplate jdbcSqlTemplate) {
        this.jdbcSqlTemplate = jdbcSqlTemplate;
        this.init();
    }

    protected void logSql(String sql, Object[] args) {
        if (log.isDebugEnabled()) {
            log.debug(sql);
            if (args != null && args.length > 0) {
                log.debug("sql args: {}", Arrays.toString(args));
            }
        }
    }

    protected void init() {
        if (this.connection != null) {
            close();
        }
        try {
            this.connection = jdbcSqlTemplate.getDataSource().getConnection();
            this.oldAutoCommitValue = this.connection.getAutoCommit();
            this.connection.setAutoCommit(false);
            SqlUtils.addSqlTransaction(this);
        } catch (SQLException ex) {
            close();
            throw jdbcSqlTemplate.translate(ex);
        }

    }

    public void setInBatchMode(boolean useBatching) {
        if (connection != null) {
            this.inBatchMode = useBatching;
        }
    }

    public boolean isInBatchMode() {
        return inBatchMode;
    }

    public void commit() {
        if (connection != null) {
            try {
                if (pstmt != null && inBatchMode) {
                    flush();
                }
                connection.commit();
            } catch (SQLException ex) {
                throw jdbcSqlTemplate.translate(ex);
            }
        }
    }

    public void rollback() {
        rollback(true);
        init();
    }

    protected void rollback(boolean clearMarkers) {
        if (connection != null) {
            try {
                if (clearMarkers) {
                    markers.clear();
                }
                connection.rollback();
            } catch (SQLException ex) {
                // do nothing
            }
        }
    }

    public void close() {
        if (connection != null) {
            JdbcSqlTemplate.close(pstmt);
            try {
                connection.setAutoCommit(this.oldAutoCommitValue);
            } catch (SQLException ex) {
                // do nothing
            }
            JdbcSqlTemplate.close(connection);
            connection = null;
            SqlUtils.removeSqlTransaction(this);
        }
    }

    public int flush() {
        int rowsUpdated = 0;
        if (markers.size() > 0 && pstmt != null) {
            try {
                int[] updates = pstmt.executeBatch();
                for (int i : updates) {
                    rowsUpdated += normalizeUpdateCount(i);
                }
                markers.clear();
            } catch (BatchUpdateException ex) {
                removeMarkersThatWereSuccessful(ex);
                throw jdbcSqlTemplate.translate(ex);
            } catch (SQLException ex) {
                throw jdbcSqlTemplate.translate(ex);
            }
        }
        return rowsUpdated;
    }

    public int queryForInt(String sql, Object... args) {
        return queryForObject(sql, Integer.class, args);
    }

    public <T> T queryForObject(final String sql, final Class<T> clazz, final Object... args) {
        return executeCallback(new IConnectionCallback<T>() {
            public T execute(Connection con) throws SQLException {
                T result = null;
                PreparedStatement ps = null;
                ResultSet rs = null;
                try {
                    ps = con.prepareStatement(sql);
                    JdbcUtils.setValues(ps, args);
                    rs = ps.executeQuery();
                    if (rs.next()) {
                        result = JdbcUtils.getObjectFromResultSet(rs, clazz);
                    }
                } finally {
                    JdbcSqlTemplate.close(rs);
                    JdbcSqlTemplate.close(ps);
                }
                return result;
            }
        });
    }

    public <T> List<T> query(String sql, ISqlRowMapper<T> mapper, Map<String, Object> namedParams) {
        ParsedSql parsedSql = NamedParameterUtils.parseSqlStatement(sql);
        String newSql = NamedParameterUtils.substituteNamedParameters(parsedSql, namedParams);
        Object[] params = NamedParameterUtils.buildValueArray(parsedSql, namedParams);
        return query(newSql, mapper, params, null);
    }

    public <T> List<T> query(final String sql, final ISqlRowMapper<T> mapper, final Object[] args,
            final int[] types) {
        return executeCallback(new IConnectionCallback<List<T>>() {
            public List<T> execute(Connection c) throws SQLException {
                PreparedStatement st = null;
                ResultSet rs = null;
                try {
                    st = c.prepareStatement(sql);
                    st.setQueryTimeout(jdbcSqlTemplate.getSettings().getQueryTimeout());
                    if (args != null) {
                        JdbcUtils.setValues(st, args, types, jdbcSqlTemplate.getLobHandler());
                    }
                    st.setFetchSize(jdbcSqlTemplate.getSettings().getFetchSize());
                    rs = st.executeQuery();
                    List<T> list = new ArrayList<T>();
                    while (rs.next()) {
                        Row row = JdbcSqlReadCursor.getMapForRow(rs);
                        T value = mapper.mapRow(row);
                        list.add(value);
                    }
                    return list;
                } finally {
                    JdbcSqlTemplate.close(rs);
                    JdbcSqlTemplate.close(st);
                }
            }
        });
    }

    public int execute(final String sql) {
        return executeCallback(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                Statement stmt = null;
                ResultSet rs = null;
                try {
                    stmt = con.createStatement();
                    if (stmt.execute(sql)) {
                        rs = stmt.getResultSet();
                        while (rs.next()) {
                        }
                    }
                    return stmt.getUpdateCount();
                } finally {
                    JdbcSqlTemplate.close(rs);
                }

            }
        });
    }

    public int prepareAndExecute(final String sql, final Object[] args, final int[] types) {
        return executeCallback(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                PreparedStatement stmt = null;
                ResultSet rs = null;
                try {
                    stmt = con.prepareStatement(sql);
                    JdbcUtils.setValues(stmt, args, types, jdbcSqlTemplate.getLobHandler());
                    if (stmt.execute()) {
                        rs = stmt.getResultSet();
                        while (rs.next()) {
                        }
                    }
                    return stmt.getUpdateCount();
                } finally {
                    JdbcSqlTemplate.close(rs);
                    JdbcSqlTemplate.close(stmt);
                }

            }
        });
    }

    public int prepareAndExecute(final String sql, final Object... args) {
        return executeCallback(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                PreparedStatement stmt = null;
                ResultSet rs = null;
                try {
                    stmt = con.prepareStatement(sql);
                    if (args != null && args.length > 0) {
                        JdbcUtils.setValues(stmt, args);
                    }
                    if (stmt.execute()) {
                        rs = stmt.getResultSet();
                        while (rs.next()) {
                        }
                    }
                    return stmt.getUpdateCount();
                } finally {
                    JdbcSqlTemplate.close(rs);
                    JdbcSqlTemplate.close(stmt);
                }

            }
        });
    }

    protected <T> T executeCallback(IConnectionCallback<T> callback) {
        try {
            return callback.execute(this.connection);
        } catch (SQLException ex) {
            throw this.jdbcSqlTemplate.translate(ex);
        }
    }

    /**
     * According to the executeUpdate() javadoc -2 means that the result was
     * successful, but that the number of rows affected is unknown. since we
     * know that only one row is suppose to be affected, we'll default to 1.
     * 
     * @param value
     */
    protected final int normalizeUpdateCount(int value) {
        if (value == Statement.SUCCESS_NO_INFO) {
            value = 1;
        }
        return value;
    }

    protected void removeMarkersThatWereSuccessful(BatchUpdateException ex) {
        int[] updateCounts = ex.getUpdateCounts();
        Iterator<Object> it = markers.iterator();
        int index = 0;
        while (it.hasNext()) {
            it.next();
            if (updateCounts.length > index && normalizeUpdateCount(updateCounts[index]) > 0) {
                it.remove();
            }
            index++;
        }
    }

    public void prepare(String sql) {
        try {
            if (this.markers.size() > 0) {
                throw new IllegalStateException(
                        "Cannot prepare a new batch before the last batch has been flushed.");
            }
            JdbcSqlTemplate.close(pstmt);
            if (log.isDebugEnabled()) {
                log.debug("Preparing: {}", sql);
            }
            pstmt = connection.prepareStatement(sql);
            psql = sql;
        } catch (SQLException ex) {
            throw jdbcSqlTemplate.translate(ex);
        }
    }

    public int addRow(Object marker, Object[] args, int[] argTypes) {
        int rowsUpdated = 0;
        try {
            if (log.isDebugEnabled()) {
                log.debug("Adding {} {}", ArrayUtils.toString(args), inBatchMode ? " in batch mode"
                        : "");
            }
            if (args != null) {
                JdbcUtils.setValues(pstmt, args, argTypes, jdbcSqlTemplate.getLobHandler());
            }
            if (inBatchMode) {
                if (marker == null) {
                    marker = new Integer(markers.size() + 1);
                }
                markers.add(marker);
                pstmt.addBatch();
                if (markers.size() >= jdbcSqlTemplate.getSettings().getBatchSize()) {
                    rowsUpdated = flush();
                }
            } else {
                rowsUpdated = pstmt.executeUpdate();
            }
        } catch (SQLException ex) {
            throw jdbcSqlTemplate.translate(ex);
        }
        return rowsUpdated;
    }

    public List<Object> getUnflushedMarkers(boolean clear) {
        List<Object> ret = new ArrayList<Object>(markers);
        if (clear) {
            markers.clear();
        }
        return ret;
    }

    public Connection getConnection() {
        return connection;
    }

    public void allowInsertIntoAutoIncrementColumns(boolean value, Table table, String quote) {
    }

    public long insertWithGeneratedKey(String sql, String column, String sequenceName,
            Object[] args, int[] types) {
        try {
            return jdbcSqlTemplate.insertWithGeneratedKey(connection, sql, column, sequenceName,
                    args, types);
        } catch (SQLException ex) {
            throw jdbcSqlTemplate.translate(ex);
        }
    }

}
