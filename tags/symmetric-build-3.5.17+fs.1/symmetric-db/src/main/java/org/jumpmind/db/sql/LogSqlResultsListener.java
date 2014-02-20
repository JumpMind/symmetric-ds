package org.jumpmind.db.sql;

import org.slf4j.Logger;

public class LogSqlResultsListener implements ISqlResultsListener {

    Logger log;

    public LogSqlResultsListener(Logger log) {
        this.log = log;
    }

    public void sqlErrored(String sql, SqlException ex, int lineNumber, boolean dropStatement,
            boolean sequenceCreate) {
        if (dropStatement || sequenceCreate) {
            log.info("DDL failed: {}", sql);
        } else {
            log.warn("DDL failed: {}", sql);
        }
    }

    public void sqlApplied(String sql, int rowsUpdated, int rowsRetrieved, int lineNumber) {
        log.info("DDL applied: {}", sql);
    }

}
