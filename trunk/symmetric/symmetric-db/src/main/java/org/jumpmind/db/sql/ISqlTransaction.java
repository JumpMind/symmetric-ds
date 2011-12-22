package org.jumpmind.db.sql;

import java.util.List;

import org.jumpmind.db.model.Table;

public interface ISqlTransaction {

    public boolean isInBatchMode();

    public void setInBatchMode(boolean batchMode);

    public void setNumberOfRowsBeforeBatchFlush(int numberOfRowsBeforeBatchFlush);

    public int getNumberOfRowsBeforeBatchFlush();
    
    public <T> T queryForObject(final String sql, Class<T> clazz, final Object... args);
    
    public int queryForInt(final String sql, final Object... args);

    public int execute(final String sql, Object... args);    
    
    public void commit();

    public void rollback();

    public void close();

    /**
     * Each time the SQL changes it needs to be submitted for preparation
     */
    public void prepare(String sql);

    public <T> int addRow(T marker, Object[] values, int[] types);

    public int flush();

    public <T> List<T> getUnflushedMarkers(boolean clear);
    
    /**
     * Indicate that the current session is to allow updates to columns that have been 
     * marked as auto increment.  This is specific to SQL Server.
     */
    public void allowInsertIntoAutoIncrementColumns(boolean value, Table table);

}
