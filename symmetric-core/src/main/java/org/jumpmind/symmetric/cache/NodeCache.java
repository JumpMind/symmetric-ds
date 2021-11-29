package org.jumpmind.symmetric.cache;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;

public class NodeCache {
    private IParameterService parameterService;
    private INodeService nodeService;
    
    private Object nodeCacheLock = new Object();
    
    volatile private Map<String, List<Node>> sourceNodesCache = new HashMap<String, List<Node>>();
    volatile private Map<String, List<Node>> targetNodesCache = new HashMap<String, List<Node>>();
    volatile private Map<String, Long> sourceNodeLinkCacheTime = new HashMap<String, Long>();
    volatile private Map<String, Long> targetNodeLinkCacheTime = new HashMap<String, Long>();
    
    public NodeCache(ISymmetricEngine engine) {
        this.parameterService = engine.getParameterService();
        this.nodeService = engine.getNodeService();
    }
    
    public List<Node> getSourceNodesCache(NodeGroupLinkAction eventAction, Node node) {
        long cacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        if (node != null) {
            List<Node> list;
            synchronized(nodeCacheLock) {
                list = sourceNodesCache.get(eventAction.name());
                if (list == null || (System.currentTimeMillis() - sourceNodeLinkCacheTime.get(eventAction.toString())) >= cacheTimeoutInMs) {
                    list = nodeService.getSourceNodesFromDatabase(eventAction, node);
                    sourceNodesCache.put(eventAction.name(), list);
                    sourceNodeLinkCacheTime.put(eventAction.toString(), System.currentTimeMillis());
                }
            }
            return list;
        } else {
            return Collections.emptyList();
        }
    }
    
    public void flushSourceNodesCache() {
        synchronized(nodeCacheLock) {
            sourceNodesCache.clear();
        }
    }
    
    public void flushTargetNodesCache() {
        synchronized(nodeCacheLock) {
            targetNodesCache.clear();
        }
    }
    
    
    public List<Node> getTargetNodesCache(NodeGroupLinkAction eventAction, Node node) {
        long cacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        if (node != null) {
            List<Node> list;
            synchronized(nodeCacheLock) {
                list = targetNodesCache.get(eventAction.name());
                if (list == null || (System.currentTimeMillis() - targetNodeLinkCacheTime.get(eventAction.toString())) >= cacheTimeoutInMs) {
                    list = nodeService.getTargetNodesFromDatabase(eventAction, node);
                    targetNodesCache.put(eventAction.name(), list);
                    targetNodeLinkCacheTime.put(eventAction.toString(), System.currentTimeMillis());
                }
            }
            return list;
        } else {
            return Collections.emptyList();
        }
    }
}
