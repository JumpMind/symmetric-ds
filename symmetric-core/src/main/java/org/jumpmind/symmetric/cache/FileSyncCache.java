package org.jumpmind.symmetric.cache;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IParameterService;

public class FileSyncCache {
    private IParameterService parameterService;
    private IFileSyncService fileSyncService;
    
    volatile private List<FileTriggerRouter> fileTriggerRoutersCache = new ArrayList<FileTriggerRouter>();
    volatile private long fileTriggerRoutersCacheTime;
    volatile private Object fileSyncCacheLock = new Object();
    
    public FileSyncCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.fileSyncService = engine.getFileSyncService();
    }
    
    public List<FileTriggerRouter> getFileTriggerRouters(boolean refreshCache) {
        long fileTriggerRouterCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS);
        if (fileTriggerRoutersCache == null || refreshCache ||
                System.currentTimeMillis() - this.fileTriggerRoutersCacheTime > fileTriggerRouterCacheTimeoutInMs) {
            synchronized (fileSyncCacheLock) {
                if (fileTriggerRoutersCache == null || refreshCache ||
                        System.currentTimeMillis() - this.fileTriggerRoutersCacheTime > fileTriggerRouterCacheTimeoutInMs) {
                    List<FileTriggerRouter> newValues = fileSyncService.getFileTriggerRoutersFromDb();
                    fileTriggerRoutersCache = newValues;
                    fileTriggerRoutersCacheTime = System.currentTimeMillis();
                }
            }
        }
        return fileTriggerRoutersCache;
    }
    
    public void flushFileTriggerRouters() {
        synchronized (fileSyncCacheLock) {
            fileTriggerRoutersCacheTime = 0l;
        }
    }
}
