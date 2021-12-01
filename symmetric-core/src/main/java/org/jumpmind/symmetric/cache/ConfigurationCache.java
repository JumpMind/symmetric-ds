package org.jumpmind.symmetric.cache;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IParameterService;

public class ConfigurationCache {
    private IParameterService parameterService;
    private IConfigurationService configurationService;
    
    private Object configurationCacheLock = new Object();
    
    volatile private Map<String, List<NodeChannel>> nodeChannelCache;
    volatile private Map<String, Channel> channelsCache;
    volatile private List<NodeGroupLink> nodeGroupLinksCache;
    volatile private Map<String, List<NodeGroupChannelWindow>> channelWindowsByChannelCache;
    volatile private long channelCacheTime;
    volatile private long nodeChannelCacheTime;
    volatile private long nodeGroupLinkCacheTime;
    volatile private long channelWindowsByChannelCacheTime;
    
    public ConfigurationCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.configurationService = engine.getConfigurationService();
    }
    
    public List<NodeChannel> getNodeChannels(String nodeId) {
        long channelCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS);
        List<NodeChannel> nodeChannels;
        synchronized(configurationCacheLock) {
            nodeChannels = nodeChannelCache != null ? nodeChannelCache.get(nodeId) : null;
            if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs || nodeChannels == null) {
                if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs || nodeChannelCache == null) {
                    nodeChannelCache = new HashMap<String, List<NodeChannel>>();
                    nodeChannelCacheTime = System.currentTimeMillis();
                }
                if (nodeId != null) {
                    nodeChannels = configurationService.getNodeChannelsFromDb(nodeId);
                    nodeChannelCache.put(nodeId, nodeChannels);
                } else {
                    nodeChannels = new ArrayList<NodeChannel>(0);
                }
            }
        }
        return nodeChannels;
    }
    
    public long getNodeChannelCacheTime() {
        return nodeChannelCacheTime;
    }
    
    public Map<String, Channel> getChannels(boolean refreshCache) {
        long channelCacheTimeoutInMs = parameterService.getLong(
                ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs
                || channelsCache == null || refreshCache) {
            synchronized (configurationCacheLock) {
                if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs
                        || channelsCache == null || refreshCache) {
                    channelsCache = configurationService.getChannelsFromDb();
                    channelCacheTime = System.currentTimeMillis();
                }
            }
        }
        return channelsCache;
    }
    
    public List<NodeGroupLink> getNodeGroupLinks(boolean refreshCache) {
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        if (System.currentTimeMillis() - nodeGroupLinkCacheTime >= cacheTimeoutInMs
                || nodeGroupLinksCache == null || refreshCache) {
            synchronized (configurationCacheLock) {
                if (System.currentTimeMillis() - nodeGroupLinkCacheTime >= cacheTimeoutInMs
                        || nodeGroupLinksCache == null || refreshCache) {
                    nodeGroupLinksCache = configurationService.getNodeGroupLinksFromDb();
                    nodeGroupLinkCacheTime = System.currentTimeMillis();
                }
            }
        }
        return nodeGroupLinksCache;
    }
    
    
    public Map<String, List<NodeGroupChannelWindow>> getNodeGroupChannelWindows() {
        long channelCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        if (System.currentTimeMillis() - channelWindowsByChannelCacheTime >= channelCacheTimeoutInMs || channelWindowsByChannelCache == null) {
            synchronized (configurationCacheLock) {
                if (System.currentTimeMillis() - channelWindowsByChannelCacheTime >= channelCacheTimeoutInMs || channelWindowsByChannelCache == null) {
                    channelWindowsByChannelCache = configurationService.getNodeGroupChannelWindowsFromDb();
                    channelWindowsByChannelCacheTime = System.currentTimeMillis();
                }
            }
        }
        return channelWindowsByChannelCache;
    }
    
    public void flushNodeChannels() {
        synchronized(configurationCacheLock) {
            this.nodeChannelCache = null;
        }
    }
    
    public void flushChannels() {
        synchronized(configurationCacheLock) {
            this.channelsCache = null;
        }
    }
    
    public void flushNodeGroupLinks() {
        synchronized(configurationCacheLock) {
            this.nodeGroupLinksCache = null;
        }
    }
    
    public void flushNodeGroupChannelWindows() {
        synchronized(configurationCacheLock) {
            this.channelWindowsByChannelCache = null;
        }
    }
}
