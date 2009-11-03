/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;

/**
 * Provides an API to configure data synchronizations.
 */
public interface IConfigurationService {

    public List<NodeGroupLink> getGroupLinks();

    public List<NodeGroupLink> getGroupLinksFor(String sourceGroupId);

    public void saveChannel(Channel channel, boolean reloadChannels);

    public void saveChannel(NodeChannel channel, boolean reloadChannels);

    public void saveNodeChannel(NodeChannel channel, boolean reloadChannels);

    public void saveNodeChannelControl(NodeChannel channel, boolean reloadChannels);

    public void deleteChannel(Channel channel);

    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String nodeGroupId, String channelId);

    public DataEventAction getDataEventActionsByGroupId(String sourceGroupId, String targetGroupId);

    public List<NodeChannel> getNodeChannels();

    public List<NodeChannel> getNodeChannels(String nodeId);

    public NodeChannel getNodeChannel(String channelId);

    public NodeChannel getNodeChannel(String channelId, String nodeId);

    public void reloadChannels();
    
    public void autoConfigDatabase(boolean force);

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

}
