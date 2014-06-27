package org.jumpmind.db.sql;

import java.util.Arrays;

public class BulkSqlException extends SqlException {

    private static final long serialVersionUID = 1L;

    private int[] failedRows;

    public BulkSqlException(int[] failedRows, String bulkOperation, String sql) {
        super(buildMessage(bulkOperation, sql, failedRows));
        this.failedRows = failedRows;
    }

    public static String buildMessage(String bulkOperation, String sql, int[] failedRows) {
        return String.format("The %s bulk operation of: %s failed. The rows that failed were: %s", bulkOperation, sql, Arrays.toString(failedRows));
    }

    public int[] getFailedRows() {
        return failedRows;
    }

}
