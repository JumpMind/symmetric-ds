package org.jumpmind.symmetric.cache;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.impl.DataLoaderService.ConflictNodeGroupLink;

public class DataLoaderCache {
    private IParameterService parameterService;
    private IDataLoaderService dataLoaderService;
    volatile private long lastConflictCacheResetTimeInMs;
    volatile private Map<NodeGroupLink, List<ConflictNodeGroupLink>> conflictSettingsCache = new HashMap<NodeGroupLink, List<ConflictNodeGroupLink>>();
    volatile private Object dataLoaderCacheLock = new Object();
    
    public DataLoaderCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.dataLoaderService = engine.getDataLoaderService();
    }
    
    public List<ConflictNodeGroupLink> getConflictSettingsNodeGroupLinks(NodeGroupLink link, boolean refreshCache) {
        long cacheTime = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_CONFLICT_IN_MS);
        if (System.currentTimeMillis() - lastConflictCacheResetTimeInMs > cacheTime || refreshCache) {
            clearDataLoaderCache();
        }
        List<ConflictNodeGroupLink> list;
        synchronized (dataLoaderCacheLock) {
            list = conflictSettingsCache.get(link);
            if (list == null) {
                list = dataLoaderService.getConflictSettinsNodeGroupLinksFromDb(link);
                conflictSettingsCache.put(link, list);
                lastConflictCacheResetTimeInMs = System.currentTimeMillis();
            }
        }
        return list;
    }
    
    public void clearDataLoaderCache() {
        synchronized (dataLoaderCacheLock) {
            conflictSettingsCache.clear();
            lastConflictCacheResetTimeInMs = 0l;
        }
    }
}
