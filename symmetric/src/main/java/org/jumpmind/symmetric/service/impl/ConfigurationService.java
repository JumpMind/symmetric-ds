/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

package org.jumpmind.symmetric.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.web.WebConstants;
import org.springframework.jdbc.core.RowMapper;

public class ConfigurationService extends AbstractService implements IConfigurationService {

    private INodeService nodeService;

    private static final long MAX_NODE_CHANNEL_CACHE_TIME = 60000;

    private static Map<String, List<NodeChannel>> nodeChannelCache;

    private static long nodeChannelCacheTime;

    private List<Channel> defaultChannels;

    public ConfigurationService() {
    }

    @SuppressWarnings("unchecked")
    public List<NodeGroupLink> getGroupLinks() {
        return jdbcTemplate.query(getSql("groupsLinksSql"), new NodeGroupLinkMapper());
    }

    @SuppressWarnings("unchecked")
    public List<NodeGroupLink> getGroupLinksFor(String nodeGroupId) {
        return jdbcTemplate.query(getSql("groupsLinksForSql"), new Object[] { nodeGroupId }, new NodeGroupLinkMapper());
    }

    public void saveChannel(Channel channel, boolean reloadChannels) {
        if (0 == jdbcTemplate.update(getSql("updateChannelSql"), new Object[] { channel.getProcessingOrder(),
                channel.getMaxBatchSize(), channel.getMaxBatchToSend(), channel.isEnabled() ? 1 : 0,
                channel.getBatchAlgorithm(), channel.getId() })) {
            jdbcTemplate.update(getSql("insertChannelSql"), new Object[] { channel.getId(),
                    channel.getProcessingOrder(), channel.getMaxBatchSize(), channel.getMaxBatchToSend(),
                    channel.isEnabled() ? 1 : 0, channel.getBatchAlgorithm() });
        }
        if (reloadChannels) {
            reloadChannels();
        }
    }

    public void saveChannel(NodeChannel nodeChannel, boolean reloadChannels) {
        saveChannel(nodeChannel.getChannel(), reloadChannels);
    }

    public void saveNodeChannel(NodeChannel nodeChannel, boolean reloadChannels) {
        saveChannel(nodeChannel.getChannel(), reloadChannels);
        saveNodeChannelControl(nodeChannel, reloadChannels);
    }

    public void saveNodeChannelControl(NodeChannel nodeChannel, boolean reloadChannels) {
        if (0 == jdbcTemplate.update(getSql("updateNodeChannelControlSql"), new Object[] {
                nodeChannel.isSuspended() ? 1 : 0, nodeChannel.isIgnored() ? 1 : 0, nodeChannel.getLastExtractedTime(),
                nodeChannel.getNodeId(), nodeChannel.getId() })) {
            jdbcTemplate.update(getSql("insertNodeChannelControlSql"), new Object[] { nodeChannel.getNodeId(),
                    nodeChannel.getId(), nodeChannel.isSuspended() ? 1 : 0, nodeChannel.isIgnored() ? 1 : 0,
                    nodeChannel.getLastExtractedTime() });
        }
        if (reloadChannels) {
            reloadChannels();
        }
    }

    public void deleteChannel(Channel channel) {
        jdbcTemplate.update(getSql("deleteChannelSql"), new Object[] { channel.getId() });
    }

    public NodeChannel getNodeChannel(String channelId) {
        return getNodeChannel(channelId, nodeService.findIdentityNodeId());
    }

    public NodeChannel getNodeChannel(String channelId, String nodeId) {
        List<NodeChannel> channels = getNodeChannels(nodeId);
        for (NodeChannel nodeChannel : channels) {
            if (nodeChannel.getId().equals(channelId)) {
                return nodeChannel;
            }
        }
        return null;
    }

    public List<NodeChannel> getNodeChannels() {
        return getNodeChannels(nodeService.findIdentityNodeId());
    }

    @SuppressWarnings("unchecked")
    public List<NodeChannel> getNodeChannels(final String nodeId) {

        if (System.currentTimeMillis() - nodeChannelCacheTime >= MAX_NODE_CHANNEL_CACHE_TIME
                || nodeChannelCache == null || nodeChannelCache.get(nodeId) == null) {
            synchronized (this) {
                if (System.currentTimeMillis() - nodeChannelCacheTime >= MAX_NODE_CHANNEL_CACHE_TIME
                        || nodeChannelCache == null || nodeChannelCache.get(nodeId) == null) {
                    if (System.currentTimeMillis() - nodeChannelCacheTime >= MAX_NODE_CHANNEL_CACHE_TIME
                            || nodeChannelCache == null) {
                        nodeChannelCache = new HashMap<String, List<NodeChannel>>();
                        nodeChannelCacheTime = System.currentTimeMillis();
                    }
                    nodeChannelCache.put(nodeId, jdbcTemplate.query(getSql("selectChannelsSql"),
                            new Object[] { nodeId }, new RowMapper() {
                                public Object mapRow(java.sql.ResultSet rs, int arg1) throws SQLException {
                                    NodeChannel nodeChannel = new NodeChannel();
                                    nodeChannel.setId(rs.getString(1));
                                    // note that 2 is intentionally missing
                                    // here.
                                    nodeChannel.setNodeId(nodeId);
                                    nodeChannel.setIgnored(isSet(rs.getObject(3)));
                                    nodeChannel.setSuspended(isSet(rs.getObject(4)));
                                    nodeChannel.setProcessingOrder(rs.getInt(5));
                                    nodeChannel.setMaxBatchSize(rs.getInt(6));
                                    nodeChannel.setEnabled(rs.getBoolean(7));
                                    nodeChannel.setMaxBatchToSend(rs.getInt(8));
                                    nodeChannel.setBatchAlgorithm(rs.getString(9));
                                    nodeChannel.setLastExtractedTime(rs.getTimestamp(10));
                                    return nodeChannel;
                                };
                            }));
                }
            }
        }
        return nodeChannelCache.get(nodeId);
    }

    public void reloadChannels() {
        nodeChannelCache = null;
    }

    private boolean isSet(Object value) {
        if (value != null && value.toString().equals("1")) {
            return true;
        } else {
            return false;
        }
    }

    public DataEventAction getDataEventActionsByGroupId(String sourceGroupId, String targetGroupId) {
        String code = (String) jdbcTemplate.queryForObject(getSql("selectDataEventActionsByIdSql"), new Object[] {
                sourceGroupId, targetGroupId }, String.class);

        return DataEventAction.fromCode(code);
    }

    public void autoConfigDatabase(boolean force) {
        if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE) || force) {
            log.info("SymmetricDSDatabaseInitializing");
            dbDialect.initSupportDb();
            if (defaultChannels != null) {
                reloadChannels();
                List<NodeChannel> channels = getNodeChannels();
                for (Channel defaultChannel : defaultChannels) {
                    if (!defaultChannel.isInList(channels)) {
                        log.info("ChannelAutoConfiguring", defaultChannel.getId());
                        saveChannel(defaultChannel, true);
                    } else {
                        log.info("ChannelExists", defaultChannel.getId());
                    }
                }
                reloadChannels();
            }
            parameterService.rereadParameters();
            log.info("SymmetricDSDatabaseInitialized");
        } else {
            log.info("SymmetricDSDatabaseNotAutoConfig");
        }
    }

    @SuppressWarnings("unchecked")
    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String nodeGroupId, String channelId) {
        return (List<NodeGroupChannelWindow>) jdbcTemplate.query(getSql("selectNodeGroupChannelWindowSql"),
                new Object[] { nodeGroupId, channelId }, new NodeGroupChannelWindowMapper());
    }

    class NodeGroupChannelWindowMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int rowNum) throws SQLException {
            NodeGroupChannelWindow window = new NodeGroupChannelWindow();
            window.setNodeGroupId(rs.getString(1));
            window.setChannelId(rs.getString(2));
            window.setStartTime(rs.getTime(3));
            window.setEndTime(rs.getTime(4));
            window.setEnabled(rs.getBoolean(5));
            return window;
        }
    }

    class NodeGroupLinkMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            NodeGroupLink node_groupTarget = new NodeGroupLink();
            node_groupTarget.setSourceGroupId(rs.getString(1));
            node_groupTarget.setTargetGroupId(rs.getString(2));
            node_groupTarget.setDataEventAction(DataEventAction.fromCode(rs.getString(3)));
            return node_groupTarget;
        }
    }

    public ChannelMap getSuspendIgnoreChannelLists(final String nodeId) {
        ChannelMap map = new ChannelMap();
        List<NodeChannel> ncs = getNodeChannels(nodeId);

        for (NodeChannel nc : ncs) {
            if (nc.isSuspended()) {
                map.addSuspendChannels(nc.getId());
            }
            if (nc.isIgnored()) {
                map.addIgnoreChannels(nc.getId());
            }
        }
        return map;
    }

    public ChannelMap getSuspendIgnoreChannelLists() {
        return getSuspendIgnoreChannelLists(nodeService.findIdentityNodeId());

    }

    public void setDefaultChannels(List<Channel> defaultChannels) {
        this.defaultChannels = defaultChannels;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}
