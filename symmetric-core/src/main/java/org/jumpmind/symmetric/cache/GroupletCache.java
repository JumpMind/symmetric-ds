package org.jumpmind.symmetric.cache;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Grouplet;
import org.jumpmind.symmetric.service.IGroupletService;
import org.jumpmind.symmetric.service.IParameterService;

public class GroupletCache {
    private IParameterService parameterService;
    private IGroupletService groupletService;
    private ISymmetricEngine engine;
    
    volatile private List<Grouplet> groupletCache;
    volatile private long groupletCacheTime = 0;
    volatile private Object groupletCacheLock = new Object();
    
    public GroupletCache(ISymmetricEngine engine) {
        this.engine = engine;
        this.parameterService = engine.getParameterService();
        this.groupletService = engine.getGroupletService();
    }
    
    public List<Grouplet> getGrouplets(boolean refreshCache) {
        if (!engine.getParameterService().is(ParameterConstants.GROUPLET_ENABLE)) {
            return new ArrayList<Grouplet>();
        }
        long maxCacheTime = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_GROUPLETS_IN_MS);
        if (groupletCache == null || System.currentTimeMillis() - groupletCacheTime >= maxCacheTime
                || groupletCacheTime == 0 || refreshCache) {
            synchronized(groupletCacheLock) {
                if (groupletCache == null || System.currentTimeMillis() - groupletCacheTime >= maxCacheTime
                        || groupletCacheTime == 0 || refreshCache) {
                    groupletCache = groupletService.getGroupletsFromDb();
                    groupletCacheTime = System.currentTimeMillis();
                }
            }
        }
        return groupletCache;
    }
    
    public void flushGrouplets() {
        synchronized(groupletCacheLock) {
            groupletCacheTime = 0l;
        }
    }
}
