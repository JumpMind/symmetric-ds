package org.jumpmind.db.sql;

public interface ISqlResultsListener {

    public void sqlApplied(String sql, int rowsUpdated, int rowsRetrieved, int lineNumber);

    public void sqlErrored(String sql, SqlException ex, int lineNumber, boolean dropStatement, boolean sequenceCreate);

}