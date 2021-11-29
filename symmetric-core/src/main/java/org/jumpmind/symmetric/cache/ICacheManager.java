package org.jumpmind.symmetric.cache;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.io.data.transform.TransformPoint;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.FileTriggerRouter;
import org.jumpmind.symmetric.model.Grouplet;
import org.jumpmind.symmetric.model.LoadFilter;
import org.jumpmind.symmetric.model.LoadFilter.LoadFilterType;
import org.jumpmind.symmetric.model.Monitor;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.Notification;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.impl.DataLoaderService.ConflictNodeGroupLink;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;

public interface ICacheManager {
    public List<TriggerRouter> getTriggerRouters(boolean refreshCache);
    public Map<String, List<TriggerRouter>> getTriggerRoutersByChannel(String nodeGroupId, boolean refreshCache);
    public Map<String, Map<Integer, TriggerRouter>> getTriggerRoutersByTriggerHist(boolean refreshCache);
    public Map<String, TriggerRouterRoutersCache> getTriggerRoutersByNodeGroupId(boolean refreshCache);
    public Map<String, Trigger> getTriggers(boolean refreshCache);
    public Map<String, Router> getRouters(boolean refreshCache);
    public Map<String, TriggerRouter> getTriggerRoutersById(boolean refreshCache);
    public void flushTriggerRoutersByNodeGroupId();
    public void flushTriggerRoutersByChannel();
    public void flushTriggerRouters();
    public void flushTriggerRoutersByTriggerHist();
    public void flushTriggerRoutersById();
    public void flushTriggers();
    public void flushRouters();
    
    public List<Node> getSourceNodesCache(NodeGroupLinkAction eventAction, Node node);
    public List<Node> getTargetNodesCache(NodeGroupLinkAction eventAction, Node node);
    public void flushSourceNodesCache();
    public void flushTargetNodesCache();
    
    public List<NodeChannel> getNodeChannels(String nodeId);
    public long getNodeChannelCacheTime();
    public Map<String, Channel> getChannels(boolean refreshCache);
    public List<NodeGroupLink> getNodeGroupLinks(boolean refreshCache);
    public Map<String, List<NodeGroupChannelWindow>> getNodeGroupChannelWindows();
    public void flushNodeChannels();
    public void flushChannels();
    public void flushNodeGroupLinks();
    public void flushNodeGroupChannelWindows();
    
    public List<ConflictNodeGroupLink> getConflictSettingsNodeGroupLinks(NodeGroupLink link, boolean refreshCache);
    public void flushConflictSettingsNodeGroupLinks();
    
    public List<FileTriggerRouter> getFileTriggerRouters(boolean refreshCache);
    public void flushFileTriggerRouters();
    
    public List<Grouplet> getGrouplets(boolean refreshCache);
    public void flushGrouplets();
    
    public Map<NodeGroupLink, Map<LoadFilterType, Map<String, List<LoadFilter>>>> findLoadFilters(NodeGroupLink nodeGroupLink,
            boolean useCache);
    public void flushLoadFilters();
    
    public List<Monitor> getActiveMonitorsForNode(String nodeGroupId, String externalId);
    public List<Monitor> getActiveMonitorsUnresolvedForNode(String nodeGroupId, String externalId);
    public void flushMonitorCache();
    public List<Notification> getActiveNotificationsForNode(String nodeGroupId, String externalId);
    public void flushNotificationCache();
    
    public Map<NodeGroupLink, Map<TransformPoint, List<TransformTableNodeGroupLink>>> getTransformCache();
    public void flushTransformCache();
}
