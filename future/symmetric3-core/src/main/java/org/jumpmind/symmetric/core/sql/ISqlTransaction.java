package org.jumpmind.symmetric.core.sql;

import java.util.List;

public interface ISqlTransaction {

    public boolean isInBatchMode();

    public void setInBatchMode(boolean batchMode);

    public void commit();

    public void rollback();

    public void close();

    /**
     * Each time the SQL changes it needs to be submitted for preparation
     */
    public void prepare(String sql, int flushSize);

    public <T> int update(T marker);
    
    public <T> int update(T marker, Object[] values, int[] types);

    public int flush();

    public <T> List<T> getUnflushedMarkers(boolean clear);

}
