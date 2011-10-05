package org.jumpmind.symmetric.core.db;

import java.util.List;

public interface ISqlTransaction {

    public boolean isInBatchMode();

    public void setInBatchMode(boolean batchMode);

    public void setNumberOfRowsBeforeBatchFlush(int numberOfRowsBeforeBatchFlush);

    public int getNumberOfRowsBeforeBatchFlush();
    
    public <T> T queryForObject(final String sql, Class<T> clazz, final Object... args);

    public void commit();

    public void rollback();

    public void close();

    /**
     * Each time the SQL changes it needs to be submitted for preparation
     */
    public void prepare(String sql);

    public <T> int update(T marker);

    public <T> int update(T marker, Object[] values, int[] types);

    public int flush();

    public <T> List<T> getUnflushedMarkers(boolean clear);
    
    public Object createSavepoint();
    
    public void releaseSavepoint(Object savePoint);
    
    public void rollback(Object savePoint);

}
