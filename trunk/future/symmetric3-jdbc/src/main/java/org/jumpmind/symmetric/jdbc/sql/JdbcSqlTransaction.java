package org.jumpmind.symmetric.jdbc.sql;

import java.sql.BatchUpdateException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.jumpmind.symmetric.core.sql.ISqlTransaction;

/**
 * TODO Support Oracle's non-standard way of batching
 */
public class JdbcSqlTransaction implements ISqlTransaction {

    protected boolean inBatchMode = true;

    protected Connection dbConnection;

    protected PreparedStatement pstmt;

    protected JdbcSqlConnection sqlConnection;

    protected int numberOfRowsBeforeBatchFlush = 1000;

    protected boolean oldAutoCommitValue;

    protected List<Object> markers = new ArrayList<Object>();

    public JdbcSqlTransaction(JdbcSqlConnection sqlConnection) {
        try {
            this.sqlConnection = sqlConnection;
            this.dbConnection = sqlConnection.getJdbcDbPlatform().getDataSource().getConnection();
            this.oldAutoCommitValue = this.dbConnection.getAutoCommit();
            this.dbConnection.setAutoCommit(false);
        } catch (SQLException ex) {
            throw sqlConnection.translate(ex);
        }
    }

    public void setInBatchMode(boolean useBatching) {
        if (dbConnection != null) {
            this.inBatchMode = useBatching && supportsBatchUpdates(dbConnection);
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
        if (dbConnection != null) {
            try {
                markers.clear();
                dbConnection.rollback();
            } catch (SQLException ex) {
                // do nothing
            }
        }
    }

    public void close() {
        if (dbConnection != null) {
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
                    rowsUpdated += i;
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

    protected void removeMarkersThatWereSuccessful(BatchUpdateException ex) {
        int[] updateCounts = ex.getUpdateCounts();
        Iterator<Object> it = markers.iterator();
        int index = 0;
        while (it.hasNext()) {
            it.next();
            if (updateCounts[index] > 0) {
                it.remove();
            }
            index++;
        }
    }

    public void prepare(String sql, int flushSize) {
        try {
            if (this.markers.size() > 0) {
                throw new IllegalStateException(
                        "Cannot prepare a new batch before the last batch has been flushed.");
            }
            this.numberOfRowsBeforeBatchFlush = flushSize;
            pstmt = dbConnection.prepareStatement(sql);
        } catch (SQLException ex) {
            throw sqlConnection.translate(ex);
        }
    }

    public int update(Object marker, Object[] values, int[] types) {
        int rowsUpdated = 0;
        try {
            StatementCreatorUtil.setValues(pstmt, values, types, sqlConnection.getJdbcDbPlatform()
                    .getLobHandler());
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

    /**
     * Return whether the given JDBC driver supports JDBC 2.0 batch updates.
     * <p>
     * Typically invoked right before execution of a given set of statements: to
     * decide whether the set of SQL statements should be executed through the
     * JDBC 2.0 batch mechanism or simply in a traditional one-by-one fashion.
     * <p>
     * Logs a warning if the "supportsBatchUpdates" methods throws an exception
     * and simply returns <code>false</code> in that case.
     * 
     * @param con
     *            the Connection to check
     * @return whether JDBC 2.0 batch updates are supported
     * @see java.sql.DatabaseMetaData#supportsBatchUpdates()
     */
    public static boolean supportsBatchUpdates(Connection con) {
        try {
            DatabaseMetaData dbmd = con.getMetaData();
            if (dbmd != null) {
                if (dbmd.supportsBatchUpdates()) {
                    return true;
                }
            }
        } catch (SQLException ex) {
        } catch (AbstractMethodError err) {
        }
        return false;
    }

}
