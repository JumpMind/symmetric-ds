package org.jumpmind.symmetric.jdbc.db;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Savepoint;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.symmetric.core.db.ISqlTransaction;
import org.jumpmind.symmetric.core.db.SqlException;

/**
 * TODO Support Oracle's non-standard way of batching
 */
public class JdbcSqlTransaction implements ISqlTransaction {

    protected boolean inBatchMode = true;

    protected Connection dbConnection;

    protected String psql;

    protected PreparedStatement pstmt;

    protected IJdbcDbDialect dbDialect;

    protected JdbcSqlTemplate sqlConnection;

    protected int numberOfRowsBeforeBatchFlush = 1000;

    protected boolean oldAutoCommitValue;

    protected List<Object> markers = new ArrayList<Object>();

    public JdbcSqlTransaction(IJdbcDbDialect platform) {
        try {
            this.dbDialect = platform;
            this.sqlConnection = platform.getJdbcSqlConnection();
            this.dbConnection = sqlConnection.getJdbcDbPlatform().getDataSource().getConnection();
            this.oldAutoCommitValue = this.dbConnection.getAutoCommit();
            this.dbConnection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw sqlConnection.translate(ex);
        }
    }

    public void setNumberOfRowsBeforeBatchFlush(int numberOfRowsBeforeBatchFlush) {
        this.numberOfRowsBeforeBatchFlush = numberOfRowsBeforeBatchFlush;
    }

    public int getNumberOfRowsBeforeBatchFlush() {
        return numberOfRowsBeforeBatchFlush;
    }

    public void setInBatchMode(boolean useBatching) {
        if (dbConnection != null) {
            this.inBatchMode = useBatching && this.dbDialect.supportsBatchUpdates();
        }
    }

    public boolean isInBatchMode() {
        return inBatchMode;
    }

    public void commit() {
        if (dbConnection != null) {
            try {
                if (pstmt != null && inBatchMode) {
                    flush();
                }
                dbConnection.commit();
            } catch (SQLException ex) {
                throw sqlConnection.translate(ex);
            }
        }
    }

    public void rollback() {
        rollback(true);
    }

    protected void rollback(boolean clearMarkers) {
        if (dbConnection != null) {
            try {
                if (clearMarkers) {
                    markers.clear();
                }
                dbConnection.rollback();
            } catch (SQLException ex) {
                // do nothing
            }
        }
    }

    public void close() {
        if (dbConnection != null) {
            JdbcSqlTemplate.close(pstmt);
            try {
                dbConnection.setAutoCommit(this.oldAutoCommitValue);
            } catch (SQLException ex) {
                // do nothing
            }
            try {
                dbConnection.close();
            } catch (SQLException ex) {
                // do nothing
            } finally {
                dbConnection = null;
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
                throw sqlConnection.translate(ex);
            } catch (SQLException ex) {
                throw sqlConnection.translate(ex);
            }
        }
        return rowsUpdated;
    }

    public <T> T queryForObject(final String sql, Class<T> clazz, final Object... args) {
        return execute(this.dbConnection, new IConnectionCallback<T>() {
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

    public <T> T execute(Connection c, IConnectionCallback<T> callback) {
        try {
            return callback.execute(c);
        } catch (SQLException ex) {
            throw this.sqlConnection.translate(ex);
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
            pstmt = dbConnection.prepareStatement(sql);
            psql = sql;
        } catch (SQLException ex) {
            throw sqlConnection.translate(ex);
        }
    }

    public int update(Object marker) {
        return update(marker, null, null);
    }

    public int update(Object marker, Object[] args, int[] argTypes) {
        int rowsUpdated = 0;
        try {
            if (args != null) {
                StatementCreatorUtil.setValues(pstmt, args, argTypes, sqlConnection
                        .getJdbcDbPlatform().getLobHandler());
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
            throw sqlConnection.translate(ex);
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

    public Object createSavepoint() {
        try {
            return dbConnection.setSavepoint();
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public void releaseSavepoint(Object savePoint) {
        try {
            dbConnection.releaseSavepoint((Savepoint) savePoint);
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

    public void rollback(Object savePoint) {
        try {
            dbConnection.rollback((Savepoint) savePoint);
        } catch (SQLException e) {
            throw new SqlException(e);
        }
    }

}
