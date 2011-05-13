package org.jumpmind.symmetric.core.sql;

import java.util.List;

public interface ISqlTransaction {

    public void setUseBatching(boolean on);
    
    public boolean isUseBatching();
    
    public void commit();
    
    public void rollback();
    
    public void close();
    
    /**
     * Each time the SQL changes it needs to be submitted for preparation
     */
    public void prepare(String sql, int flushSize);
    
    public <T> int update(T marker, Object[] values, int[] types);
    
    public <T> List<T> getFailedMarkers();
    
}
