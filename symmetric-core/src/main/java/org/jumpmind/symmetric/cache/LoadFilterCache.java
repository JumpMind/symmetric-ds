package org.jumpmind.symmetric.cache;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.LoadFilter.LoadFilterType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.ILoadFilterService;
import org.jumpmind.symmetric.service.IParameterService;

public class LoadFilterCache {
    private IParameterService parameterService;
    private ILoadFilterService loadFilterService;
    
    volatile private Map<NodeGroupLink, Map<LoadFilterType, Map<String, List<LoadFilter>>>> loadFilterCache;
    volatile private long loadFilterCacheTime = 0;
    volatile private Object loadFilterCacheLock = new Object();
    
    public LoadFilterCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.loadFilterService = engine.getLoadFilterService();
    }
    
    public Map<NodeGroupLink, Map<LoadFilterType, Map<String, List<LoadFilter>>>> findLoadFilters(NodeGroupLink nodeGroupLink,
            boolean useCache) {
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_LOAD_FILTER_IN_MS);
        if (System.currentTimeMillis() - loadFilterCacheTime >= cacheTimeoutInMs
                || loadFilterCache == null) {
            synchronized (loadFilterCacheLock) {
                if (System.currentTimeMillis() - loadFilterCacheTime >= cacheTimeoutInMs
                        || loadFilterCache == null) {
                    loadFilterCache = loadFilterService.findLoadFiltersFromDb();
                    loadFilterCacheTime = System.currentTimeMillis();
                }
            }
        }
        return loadFilterCache;
    }
    
    public void flushLoadFilterCache() {
        synchronized(loadFilterCacheLock) {
            loadFilterCache = null;
        }
    }
}
