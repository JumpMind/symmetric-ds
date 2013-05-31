package org.jumpmind.symmetric.service;

import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroup;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;

/**
 * Provides an API to configure data synchronizations.
 */
public interface IConfigurationService {
    
    public boolean refreshFromDatabase();

    public List<NodeGroup> getNodeGroups();
    
    public void saveNodeGroup(NodeGroup group);
    
    public void saveNodeGroupLink(NodeGroupLink link);
    
    public void deleteNodeGroup(String nodeGroupId);
    
    public void deleteNodeGroupLink(NodeGroupLink link);
    
    public List<NodeGroupLink> getNodeGroupLinks();

    public List<NodeGroupLink> getNodeGroupLinksFor(String sourceGroupId);
    
    public NodeGroupLink getNodeGroupLinkFor(String sourceNodeGroupId, String targetNodeGroupId);
    
    /**
     * Check to see if the channel is currently being used in the system.
     */
    public boolean isChannelInUse(String channelId);
    
    public void saveChannel(Channel channel, boolean reloadChannels);

    public void saveChannel(NodeChannel channel, boolean reloadChannels);

    public void saveNodeChannel(NodeChannel channel, boolean reloadChannels);

    public void saveNodeChannelControl(NodeChannel channel, boolean reloadChannels);

    public void deleteChannel(Channel channel);

    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String nodeGroupId, String channelId);

    public NodeGroupLinkAction getDataEventActionByGroupLinkId(String sourceGroupId, String targetGroupId);

    public List<NodeChannel> getNodeChannels(boolean refreshExtractMillis);

    public List<NodeChannel> getNodeChannels(String nodeId, boolean refreshExtractMillis);

    public NodeChannel getNodeChannel(String channelId, boolean refreshExtractMillis);
    
    public Channel getChannel (String channelId);
    
    public Map<String, Channel> getChannels(boolean refreshCache);

    public NodeChannel getNodeChannel(String channelId, String nodeId, boolean refreshExtractMillis);

    public void clearCache();
    
    public void initDefaultChannels();

    /**
     * Returns two sets of channel names, one for suspended channels and one for
     * ignored.
     * 
     * @param nodeId
     * @return A Map with two entries, the sets of which will always be defined
     *         but may be empty.
     */
    public ChannelMap getSuspendIgnoreChannelLists(String nodeId);

    public ChannelMap getSuspendIgnoreChannelLists();
    
    /**
     * @return a map of nodes to redirect to that is keyed by a list of external_ids that should be redirected.
     */
    public Map<String,String> getRegistrationRedirectMap();

}