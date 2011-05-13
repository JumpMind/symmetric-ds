package org.jumpmind.symmetric.jdbc.sql;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.core.sql.ISqlTransaction;

/**
 * TODO Support Oracle's non-standard way of batching
 */
public class JdbcSqlTransaction implements ISqlTransaction {

    protected boolean useBatching = true;

    protected Connection dbConnection;

    protected PreparedStatement pstmt;

    protected ISqlConnection sqlConnection;

    public JdbcSqlTransaction(DataSource dataSource, JdbcSqlConnection sqlConnection) {
        try {
            this.dbConnection = dataSource.getConnection();
        } catch (SQLException ex) {
            throw sqlConnection.translate(ex);
        }
    }

    public void setUseBatching(boolean useBatching) {
        this.useBatching = useBatching;
    }

    public boolean isUseBatching() {
        return useBatching;
    }

    public void commit() {
        if (dbConnection != null) {
            try {
                if (pstmt != null && useBatching) {
                    pstmt.executeBatch();
                }
                dbConnection.commit();

            } catch (SQLException ex) {

            } finally {
                dbConnection = null;
            }
        }
    }

    public void rollback() {
        if (dbConnection != null) {
            try {
                dbConnection.rollback();
            } catch (SQLException ex) {

            } finally {
                dbConnection = null;
            }
        }
    }

    public void close() {
        if (dbConnection != null) {
            try {
                dbConnection.close();
            } catch (SQLException ex) {

            } finally {
                dbConnection = null;
            }
        }
    }

    public void prepare(String sql, int flushSize) {
        // TODO Auto-generated method stub

    }

    public <T> int update(T marker, Object[] values, int[] types) {
        // TODO Auto-generated method stub
        return 0;
    }

    public <T> List<T> getFailedMarkers() {
        // TODO Auto-generated method stub
        return null;
    }

}
