package org.jumpmind.symmetric.jdbc.db.postgres;

import java.sql.BatchUpdateException;
import java.sql.SQLException;

import org.jumpmind.symmetric.jdbc.db.IJdbcDbDialect;
import org.jumpmind.symmetric.jdbc.db.JdbcSqlTransaction;

public class PostgresJdbcSqlTransaction extends JdbcSqlTransaction {

    public PostgresJdbcSqlTransaction(IJdbcDbDialect platform) {
        super(platform);
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
                rollback(false);
                throw sqlConnection.translate(ex);
            } catch (SQLException ex) {
                throw sqlConnection.translate(ex);
            }
        }
        return rowsUpdated;
    }


}
