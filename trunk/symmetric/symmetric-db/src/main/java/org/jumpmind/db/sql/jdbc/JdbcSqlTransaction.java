package org.jumpmind.db.sql.jdbc;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.db.model.Table;
import org.jumpmind.db.sql.ISqlTransaction;

/**
 * TODO Support Oracle's non-standard way of batching
 */
public class JdbcSqlTransaction implements ISqlTransaction {

    protected boolean inBatchMode = false;

    protected Connection connection;

    protected String psql;

    protected PreparedStatement pstmt;

    protected JdbcSqlTemplate jdbcSqlTemplate;

    protected int numberOfRowsBeforeBatchFlush = 1000;

    protected boolean oldAutoCommitValue;

    protected List<Object> markers = new ArrayList<Object>();

    public JdbcSqlTransaction(JdbcSqlTemplate jdbcSqlTemplate) {
        try {
            this.jdbcSqlTemplate = jdbcSqlTemplate;
            this.connection = jdbcSqlTemplate.getDataSource().getConnection();
            this.oldAutoCommitValue = this.connection.getAutoCommit();
            this.connection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw jdbcSqlTemplate.translate(ex);
        }
    }

    public void setNumberOfRowsBeforeBatchFlush(int numberOfRowsBeforeBatchFlush) {
        this.numberOfRowsBeforeBatchFlush = numberOfRowsBeforeBatchFlush;
    }

    public int getNumberOfRowsBeforeBatchFlush() {
        return numberOfRowsBeforeBatchFlush;
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
            try {
                connection.close();
            } catch (SQLException ex) {
                // do nothing
            } finally {
                connection = null;
            }
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
                    JdbcSqlTemplate.close(rs);
                    JdbcSqlTemplate.close(ps);
                }
                return result;
            }
        });
    }

    public int execute(final String sql, final Object... args) {
        return execute(new IConnectionCallback<Integer>() {
            public Integer execute(Connection con) throws SQLException {
                PreparedStatement stmt = null;
                ResultSet rs = null;
                try {
                    stmt = con.prepareStatement(sql);
                    StatementCreatorUtil.setValues(stmt, args);
                    if (stmt.execute()) {
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

    protected <T> T execute(IConnectionCallback<T> callback) {
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
            pstmt = connection.prepareStatement(sql);
            psql = sql;
        } catch (SQLException ex) {
            throw jdbcSqlTemplate.translate(ex);
        }
    }

    public int addRow(Object marker, Object[] args, int[] argTypes) {
        int rowsUpdated = 0;
        try {
            if (args != null) {
                StatementCreatorUtil.setValues(pstmt, args, argTypes,
                        jdbcSqlTemplate.getLobHandler());
            }
            if (inBatchMode) {
                if (marker == null) {
                    marker = new Integer(markers.size() + 1);
                }
                markers.add(marker);
                pstmt.addBatch();
                if (markers.size() >= numberOfRowsBeforeBatchFlush) {
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

    public void allowInsertIntoAutoIncrementColumns(boolean value, Table table) {
    }

}
