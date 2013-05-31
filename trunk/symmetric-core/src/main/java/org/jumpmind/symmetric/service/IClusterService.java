package org.jumpmind.symmetric.service;

import java.util.Map;

import org.jumpmind.symmetric.model.Lock;


/**
 * Service API that is responsible for acquiring distributed locks for 
 * clustered SymmetricDS nodes.
 */
public interface IClusterService {

    public void init();    
    
    public void initLockTable(final String action);

    public boolean lock(String action);
    
    public void unlock(String action);
    
    public void clearAllLocks();
    
    public String getServerId();
    
    public boolean isClusteringEnabled();
    
    public Map<String,Lock> findLocks();
    
    public void aquireInfiniteLock(String action);
    
    public void clearInfiniteLock(String action);
    
    public boolean isInfiniteLocked(String action);

}