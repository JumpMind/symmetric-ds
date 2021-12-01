package org.jumpmind.symmetric.cache;

import java.util.List;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.service.IMonitorService;
import org.jumpmind.symmetric.service.IParameterService;

public class MonitorCache {
    private IParameterService parameterService;
    private IMonitorService monitorService;
    
    volatile private List<Monitor> activeMonitorCache;
    volatile private long activeMonitorCacheTime;
    volatile private List<Monitor> activeUnresolvedMonitorCache;
    volatile private long activeUnresolvedMonitorCacheTime;
    volatile private List<Notification> activeNotificationCache;
    volatile private long activeNotificationCacheTime;
    
    volatile private Object monitorCacheLock = new Object();
    
    public MonitorCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.monitorService = engine.getMonitorService();
    }
    
    public List<Monitor> getMonitorsForNode(String nodeGroupId, String externalId) {
        long cacheTimeout = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_MONITOR_IN_MS);
        if (activeMonitorCache == null || System.currentTimeMillis() - activeMonitorCacheTime > cacheTimeout) {
            synchronized(monitorCacheLock) {
                if (activeMonitorCache == null || System.currentTimeMillis() - activeMonitorCacheTime > cacheTimeout) {
                    activeMonitorCache = monitorService.getActiveMonitorsForNodeFromDb(nodeGroupId, externalId);
                    activeMonitorCacheTime = System.currentTimeMillis();
                }
            }
        }
        return activeMonitorCache;
    }
    
    public List<Monitor> getMonitorsUnresolvedForNode(String nodeGroupId, String externalId) {
        long cacheTimeout = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_MONITOR_IN_MS);
        if (activeUnresolvedMonitorCache == null || System.currentTimeMillis() - activeUnresolvedMonitorCacheTime > cacheTimeout) {
            synchronized(monitorCacheLock) {
                if (activeUnresolvedMonitorCache == null || System.currentTimeMillis() - activeUnresolvedMonitorCacheTime > cacheTimeout) {
                    activeUnresolvedMonitorCache = monitorService.getActiveMonitorsUnresolvedForNodeFromDb(nodeGroupId, externalId);
                    activeUnresolvedMonitorCacheTime = System.currentTimeMillis();
                }
            }
        }
        return activeUnresolvedMonitorCache;
    }
    
    public void flushMonitorCache() {
        synchronized(monitorCacheLock) {
            activeMonitorCache = null;
        }
    }
    
    public List<Notification> getNotificationsForNode(String nodeGroupId, String externalId) {
        long cacheTimeout = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NOTIFICATION_IN_MS);
        if (activeNotificationCache == null || System.currentTimeMillis() - activeNotificationCacheTime > cacheTimeout) {
            synchronized(monitorCacheLock) {
                if (activeNotificationCache == null || System.currentTimeMillis() - activeNotificationCacheTime > cacheTimeout) {
                    activeNotificationCache = monitorService.getActiveNotificationsForNodeFromDb(nodeGroupId, externalId);
                    activeNotificationCacheTime = System.currentTimeMillis();
                }
            }
        }
        return activeNotificationCache;
    }
    
    public void flushNotificationCache() {
        synchronized(monitorCacheLock) {
            activeNotificationCache = null;
        }
    }
}
