/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroup;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * @see IConfigurationService
 */
public class ConfigurationService extends AbstractService implements IConfigurationService {

    private INodeService nodeService;

    private Map<String, List<NodeChannel>> nodeChannelCache;

    private Map<String, Channel> channelsCache;

    private List<NodeGroupLink> nodeGroupLinksCache;

    private long channelCacheTime;

    private long nodeChannelCacheTime;

    private long nodeGroupLinkCacheTime;

    private List<Channel> defaultChannels;
    
    private Map<String, List<NodeGroupChannelWindow>> channelWindowsByChannelCache;

    private Date lastUpdateTime;

    public ConfigurationService(IParameterService parameterService, ISymmetricDialect dialect,
            INodeService nodeService) {
        super(parameterService, dialect);
        this.nodeService = nodeService;
        this.defaultChannels = new ArrayList<Channel>();
        this.defaultChannels
                .add(new Channel(Constants.CHANNEL_CONFIG, 0, 2000, 100, true, 0, true));
        this.defaultChannels.add(new Channel(Constants.CHANNEL_RELOAD, 1, 1, 1, true, 0, false,
                true, false));
        this.defaultChannels.add(new Channel(Constants.CHANNEL_HEARTBEAT, 2, 100, 100, true, 0,
                false));
        this.defaultChannels.add(new Channel(Constants.CHANNEL_DEFAULT, 99999, 1000, 100, true, 0,
                false));
        this.defaultChannels.add(new Channel(Constants.CHANNEL_DYNAMIC, 99999, 1000, 100, true, 0,
                false));
        if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)) {
            this.defaultChannels.add(new Channel(Constants.CHANNEL_FILESYNC, 3, 100, 100, true, 0,
                    false, "nontransactional", false, true));
            this.defaultChannels.add(new Channel(Constants.CHANNEL_FILESYNC_RELOAD, 1, 100, 100,
                    true, 0, false, "nontransactional", true, true));
        }
        setSqlMap(new ConfigurationServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }
    
    public boolean isMasterToMaster() {
        boolean masterToMaster = false;
        Node me = nodeService.findIdentity();
        if (me != null) {
            masterToMaster = getNodeGroupLinkFor(me.getNodeGroupId(), me.getNodeGroupId(), false) != null;
        }
        return masterToMaster;
    }

    public boolean isMasterToMasterOnly() {
        boolean masterToMasterOnly = false;
        Node me = nodeService.findIdentity();
        if (me != null) {
            masterToMasterOnly = sqlTemplate.queryForInt(getSql("countGroupLinksForSql"), me.getNodeGroupId(), me.getNodeGroupId()) == 1;
        }
        return masterToMasterOnly;
    }

    public boolean refreshFromDatabase() {
        Date date1 = sqlTemplate.queryForObject(getSql("selectMaxChannelLastUpdateTime"),
                Date.class);
        Date date2 = sqlTemplate.queryForObject(getSql("selectMaxNodeGroupLastUpdateTime"),
                Date.class);
        Date date3 = sqlTemplate.queryForObject(getSql("selectMaxNodeGroupLinkLastUpdateTime"),
                Date.class);
        Date date = maxDate(date1, date2, date3);

        if (date != null) {
            if (lastUpdateTime == null || lastUpdateTime.before(date)) {
                if (lastUpdateTime != null) {
                    log.info("Newer channel or group settings were detected");
                }
                lastUpdateTime = date;
                clearCache();
                return true;
            }
        }
        return false;
    }

    public void saveNodeGroupLink(NodeGroupLink link) {
        if (!doesNodeGroupExist(link.getSourceNodeGroupId())) {
            saveNodeGroup(new NodeGroup(link.getSourceNodeGroupId()));
        }

        if (!doesNodeGroupExist(link.getTargetNodeGroupId())) {
            saveNodeGroup(new NodeGroup(link.getTargetNodeGroupId()));
        }

        link.setLastUpdateTime(new Date());
        if (sqlTemplate.update(getSql("updateNodeGroupLinkSql"), link.getDataEventAction().name(),
                link.isSyncConfigEnabled() ? 1 : 0, link.getLastUpdateTime(),
                link.getLastUpdateBy(), link.getSourceNodeGroupId(), link.getTargetNodeGroupId()) == 0) {
            link.setCreateTime(new Date());
            sqlTemplate.update(getSql("insertNodeGroupLinkSql"), link.getDataEventAction().name(),
                    link.getSourceNodeGroupId(), link.getTargetNodeGroupId(),
                    link.isSyncConfigEnabled() ? 1 : 0, link.getLastUpdateTime(),
                    link.getLastUpdateBy(), link.getCreateTime());
        }
    }

    public boolean doesNodeGroupExist(String nodeGroupId) {
        boolean exists = false;
        List<NodeGroup> groups = getNodeGroups();
        for (NodeGroup nodeGroup : groups) {
            exists |= nodeGroup.getNodeGroupId().equals(nodeGroupId);
        }
        return exists;
    }

    public void saveNodeGroup(NodeGroup group) {
        group.setLastUpdateTime(new Date());
        if (sqlTemplate.update(getSql("updateNodeGroupSql"), group.getDescription(),
                group.getLastUpdateTime(), group.getLastUpdateBy(), group.getNodeGroupId()) == 0) {
            group.setCreateTime(new Date());
            sqlTemplate.update(getSql("insertNodeGroupSql"), group.getDescription(),
                    group.getNodeGroupId(), group.getLastUpdateTime(), group.getLastUpdateBy(),
                    group.getCreateTime());
        }
    }

    public void deleteNodeGroup(String nodeGroupId) {
        sqlTemplate.update(getSql("deleteNodeGroupSql"), nodeGroupId);
    }

    public void deleteNodeGroupLink(NodeGroupLink link) {
        sqlTemplate.update(getSql("deleteNodeGroupLinkSql"), link.getSourceNodeGroupId(),
                link.getTargetNodeGroupId());
    }

    public List<NodeGroup> getNodeGroups() {
        return sqlTemplate.query(getSql("selectNodeGroupsSql"), new NodeGroupMapper());
    }

    public List<NodeGroupLink> getNodeGroupLinks(boolean refreshCache) {
        if (refreshCache) {
            nodeGroupLinkCacheTime = 0;
        }
        long cacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        List<NodeGroupLink> links = nodeGroupLinksCache;
        if (System.currentTimeMillis() - nodeGroupLinkCacheTime >= cacheTimeoutInMs
                || links == null) {
            synchronized (this) {
                links = nodeGroupLinksCache;
                if (System.currentTimeMillis() - nodeGroupLinkCacheTime >= cacheTimeoutInMs
                        || links == null) {
                    links = sqlTemplate.query(getSql("groupsLinksSql"), new NodeGroupLinkMapper());
                    nodeGroupLinksCache = links;
                    nodeGroupLinkCacheTime = System.currentTimeMillis();
                }
            }
        }

        return links;
    }

    public List<NodeGroupLink> getNodeGroupLinksFor(String sourceNodeGroupId, boolean refreshCache) {
        List<NodeGroupLink> links = getNodeGroupLinks(refreshCache);
        List<NodeGroupLink> target = new ArrayList<NodeGroupLink>(links.size());
        for (NodeGroupLink nodeGroupLink : links) {
            if (nodeGroupLink.getSourceNodeGroupId().equals(sourceNodeGroupId)) {
                target.add(nodeGroupLink);
            }
        }
        return target;
    }

    public NodeGroupLink getNodeGroupLinkFor(String sourceNodeGroupId, String targetNodeGroupId,
            boolean refreshCache) {
        List<NodeGroupLink> links = getNodeGroupLinks(refreshCache);
        for (NodeGroupLink nodeGroupLink : links) {
            if (nodeGroupLink.getTargetNodeGroupId().equals(targetNodeGroupId)
                    && nodeGroupLink.getSourceNodeGroupId().equals(sourceNodeGroupId)) {
                return nodeGroupLink;
            }
        }
        return null;
    }

    public boolean isChannelInUse(String channelId) {
        return sqlTemplate.queryForInt(getSql("isChannelInUseSql"), channelId) > 0;
    }

    public void saveChannel(Channel channel, boolean reloadChannels) {
        channel.setLastUpdateTime(new Date());
        if (0 == sqlTemplate.update(
                getSql("updateChannelSql"),
                new Object[] { channel.getProcessingOrder(), channel.getMaxBatchSize(),
                        channel.getMaxBatchToSend(), channel.getMaxDataToRoute(),
                        channel.isUseOldDataToRoute() ? 1 : 0,
                        channel.isUseRowDataToRoute() ? 1 : 0,
                        channel.isUsePkDataToRoute() ? 1 : 0, channel.isContainsBigLob() ? 1 : 0,
                        channel.isEnabled() ? 1 : 0, channel.getBatchAlgorithm(),
                        channel.getExtractPeriodMillis(), channel.getDataLoaderType(),
                        channel.getLastUpdateTime(), channel.getLastUpdateBy(),
                        channel.isReloadFlag() ? 1 : 0, channel.isFileSyncFlag() ? 1 : 0,
                        channel.getChannelId() })) {
            channel.setCreateTime(new Date());
            sqlTemplate.update(
                    getSql("insertChannelSql"),
                    new Object[] { channel.getChannelId(), channel.getProcessingOrder(),
                            channel.getMaxBatchSize(), channel.getMaxBatchToSend(),
                            channel.getMaxDataToRoute(), channel.isUseOldDataToRoute() ? 1 : 0,
                            channel.isUseRowDataToRoute() ? 1 : 0,
                            channel.isUsePkDataToRoute() ? 1 : 0,
                            channel.isContainsBigLob() ? 1 : 0, channel.isEnabled() ? 1 : 0,
                            channel.getBatchAlgorithm(), channel.getExtractPeriodMillis(),
                            channel.getDataLoaderType(), channel.getLastUpdateTime(),
                            channel.getLastUpdateBy(), channel.getCreateTime(),
                            channel.isReloadFlag() ? 1 : 0, channel.isFileSyncFlag() ? 1 : 0, });
        }
        if (reloadChannels) {
            clearCache();
        }
    }

    public void saveChannel(NodeChannel nodeChannel, boolean reloadChannels) {
        saveChannel(nodeChannel.getChannel(), reloadChannels);
    }

    public void saveNodeChannel(NodeChannel nodeChannel, boolean reloadChannels) {
        saveChannel(nodeChannel.getChannel(), false);
        saveNodeChannelControl(nodeChannel, reloadChannels);
    }

    public void saveNodeChannelControl(NodeChannel nodeChannel, boolean reloadChannels) {
        if (0 == sqlTemplate.update(
                getSql("updateNodeChannelControlSql"),
                new Object[] { nodeChannel.isSuspendEnabled() ? 1 : 0,
                        nodeChannel.isIgnoreEnabled() ? 1 : 0, nodeChannel.getLastExtractTime(),
                        nodeChannel.getNodeId(), nodeChannel.getChannelId() })) {
            sqlTemplate
                    .update(getSql("insertNodeChannelControlSql"),
                            new Object[] { nodeChannel.getNodeId(), nodeChannel.getChannelId(),
                                    nodeChannel.isSuspendEnabled() ? 1 : 0,
                                    nodeChannel.isIgnoreEnabled() ? 1 : 0,
                                    nodeChannel.getLastExtractTime() });
        }
        if (reloadChannels) {
            clearCache();
        }
    }

    public void deleteChannel(Channel channel) {
        sqlTemplate.update(getSql("deleteNodeChannelSql"), new Object[] { channel.getChannelId() });
        sqlTemplate.update(getSql("deleteChannelSql"), new Object[] { channel.getChannelId() });
        clearCache();
    }

    public NodeChannel getNodeChannel(String channelId, boolean refreshExtractMillis) {
        return getNodeChannel(channelId, nodeService.findIdentityNodeId(), refreshExtractMillis);
    }

    public NodeChannel getNodeChannel(String channelId, String nodeId, boolean refreshExtractMillis) {
        List<NodeChannel> channels = getNodeChannels(nodeId, refreshExtractMillis);
        for (NodeChannel nodeChannel : channels) {
            if (nodeChannel.getChannelId().equals(channelId)) {
                return nodeChannel;
            }
        }
        return null;
    }

    public List<NodeChannel> getNodeChannels(boolean refreshExtractMillis) {
        return getNodeChannels(nodeService.findIdentityNodeId(), refreshExtractMillis);
    }

    public List<NodeChannel> getNodeChannels(final String nodeId, boolean refreshExtractMillis) {
        boolean loaded = false;
        long channelCacheTimeoutInMs = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS);
        List<NodeChannel> nodeChannels = nodeChannelCache != null ? nodeChannelCache.get(nodeId)
                : null;
        if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                || nodeChannels == null) {
            synchronized (this) {
                if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                        || nodeChannelCache == null || nodeChannelCache.get(nodeId) == null
                        || nodeChannels == null) {
                    if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                            || nodeChannelCache == null) {
                        nodeChannelCache = new HashMap<String, List<NodeChannel>>();
                        nodeChannelCacheTime = System.currentTimeMillis();
                    }

                    if (nodeId != null) {
                        nodeChannels = sqlTemplate.query(getSql("selectNodeChannelsSql"),
                                new ISqlRowMapper<NodeChannel>() {
                                    public NodeChannel mapRow(Row row) {
                                        NodeChannel nodeChannel = new NodeChannel();
                                        nodeChannel.setChannelId(row.getString("channel_id"));
                                        nodeChannel.setNodeId(nodeId);
                                        nodeChannel.setIgnoreEnabled(row
                                                .getBoolean("ignore_enabled"));
                                        nodeChannel.setSuspendEnabled(row
                                                .getBoolean("suspend_enabled"));
                                        nodeChannel.setProcessingOrder(row
                                                .getInt("processing_order"));
                                        nodeChannel.setMaxBatchSize(row.getInt("max_batch_size"));
                                        nodeChannel.setEnabled(row.getBoolean("enabled"));
                                        nodeChannel.setMaxBatchToSend(row
                                                .getInt("max_batch_to_send"));
                                        nodeChannel.setMaxDataToRoute(row
                                                .getInt("max_data_to_route"));
                                        nodeChannel.setUseOldDataToRoute(row
                                                .getBoolean("use_old_data_to_route"));
                                        nodeChannel.setUseRowDataToRoute(row
                                                .getBoolean("use_row_data_to_route"));
                                        nodeChannel.setUsePkDataToRoute(row
                                                .getBoolean("use_pk_data_to_route"));
                                        nodeChannel.setContainsBigLob(row
                                                .getBoolean("contains_big_lob"));
                                        nodeChannel.setBatchAlgorithm(row
                                                .getString("batch_algorithm"));
                                        nodeChannel.setLastExtractTime(row
                                                .getDateTime("last_extract_time"));
                                        nodeChannel.setExtractPeriodMillis(row
                                                .getLong("extract_period_millis"));
                                        nodeChannel.setDataLoaderType(row
                                                .getString("data_loader_type"));
                                        nodeChannel.setCreateTime(row.getDateTime("create_time"));
                                        nodeChannel.setLastUpdateBy(row.getString("last_update_by"));
                                        nodeChannel.setLastUpdateTime(row
                                                .getDateTime("last_update_time"));
                                        nodeChannel.setFileSyncFlag(row
                                                .getBoolean("file_sync_flag"));
                                        nodeChannel.setReloadFlag(row.getBoolean("reload_flag"));
                                        return nodeChannel;
                                    };
                                }, nodeId);
                        nodeChannelCache.put(nodeId, nodeChannels);
                        loaded = true;
                    } else {
                        nodeChannels = new ArrayList<NodeChannel>(0);
                    }
                }
            }
        }

        if (!loaded && refreshExtractMillis) {
            /*
             * need to read last extracted time from database regardless of
             * whether we used the cache or not. locate the nodes in the cache,
             * and update it.
             */
            final Map<String, NodeChannel> nodeChannelsMap = new HashMap<String, NodeChannel>();

            for (NodeChannel nc : nodeChannels) {
                nodeChannelsMap.put(nc.getChannelId(), nc);
            }

            sqlTemplate.query(getSql("selectNodeChannelControlLastExtractTimeSql"),
                    new ISqlRowMapper<Object>() {
                        public Object mapRow(Row row) {
                            String channelId = row.getString("channel_id");
                            Date extractTime = row.getDateTime("last_extract_time");
                            NodeChannel nodeChannel = nodeChannelsMap.get(channelId);
                            if (nodeChannel != null) {
                                nodeChannel.setLastExtractTime(extractTime);
                            }
                            return nodeChannelsMap;
                        };
                    }, nodeId);

        }

        return nodeChannels;
    }

    public void clearCache() {
        synchronized (this) {
            nodeChannelCache = null;
            channelsCache = null;
            nodeGroupLinksCache = null;
            channelWindowsByChannelCache = null;
        }
    }

    public NodeGroupLinkAction getDataEventActionByGroupLinkId(String sourceGroupId,
            String targetGroupId) {
        String code = (String) sqlTemplate.queryForObject(getSql("selectDataEventActionsByIdSql"),
                String.class, sourceGroupId, targetGroupId);
        return NodeGroupLinkAction.fromCode(code);
    }

    public void initDefaultChannels() {
        if (defaultChannels != null) {
            clearCache();
            List<NodeChannel> channels = getNodeChannels(false);
            for (Channel defaultChannel : defaultChannels) {
                Channel channel = defaultChannel.findInList(channels);
                if (channel == null) {
                    log.info("Auto-configuring {} channel", defaultChannel.getChannelId());
                    saveChannel(defaultChannel, true);
                } else if (channel.getChannelId().equals(Constants.CHANNEL_RELOAD)
                        && !channel.isReloadFlag()) {
                    log.info("Setting reload flag on reload channel");
                    channel.setReloadFlag(true);
                    saveChannel(channel, true);
                } else if (channel.getChannelId().equals(Constants.CHANNEL_FILESYNC)
                        && !channel.isFileSyncFlag()) {
                    log.info("Setting file sync flag on file sync channel");
                    channel.setFileSyncFlag(true);
                    saveChannel(channel, true);
                } else if (channel.getChannelId().equals(Constants.CHANNEL_FILESYNC_RELOAD)
                        && (!channel.isFileSyncFlag() || !channel.isReloadFlag())) {
                    log.info("Setting reload and file sync flag on file sync reload channel");
                    channel.setFileSyncFlag(true);
                    saveChannel(channel, true);
                    
                } else {
                    log.debug("No need to create channel {}.  It already exists",
                            defaultChannel.getChannelId());
                }
            }
            clearCache();
        }
    }

    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String notUsed, String channelId) {
        long channelCacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        Map<String, List<NodeGroupChannelWindow>> channelWindowsByChannel = channelWindowsByChannelCache;
        if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs || channelWindowsByChannel == null) {
            synchronized (this) {
                channelWindowsByChannel = channelWindowsByChannelCache;
                if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs || channelWindowsByChannel == null) {
                    channelWindowsByChannel = new HashMap<String, List<NodeGroupChannelWindow>>();
                    String nodeGroupId = parameterService.getNodeGroupId();
                    Set<String> channelIds = getChannels(false).keySet();
                    for (String id : channelIds) {
                        channelWindowsByChannel.put(channelId, sqlTemplate.query(getSql("selectNodeGroupChannelWindowSql"),
                                new NodeGroupChannelWindowMapper(), nodeGroupId, id));
                    }

                    channelWindowsByChannelCache = channelWindowsByChannel;
                }
            }
        }
        return channelWindowsByChannel.get(channelId);
    }

    public ChannelMap getSuspendIgnoreChannelLists(final String nodeId) {
        ChannelMap map = new ChannelMap();
        List<NodeChannel> ncs = getNodeChannels(nodeId, true);

        if (ncs != null) {
            for (NodeChannel nc : ncs) {
                if (nc.isSuspendEnabled()) {
                    map.addSuspendChannels(nc.getChannelId());
                }
                if (nc.isIgnoreEnabled()) {
                    map.addIgnoreChannels(nc.getChannelId());
                }
            }
        }
        return map;
    }

    public List<Channel> getFileSyncChannels() {
        List<Channel> list = new ArrayList<Channel>(getChannels(false).values());
        Iterator<Channel> it = list.iterator();
        while (it.hasNext()) {
            Channel channel = it.next();
            if (!channel.isFileSyncFlag()) {
                it.remove();
            }
        }
        return list;
    }

    public Map<String, Channel> getChannels(boolean refreshCache) {
        long channelCacheTimeoutInMs = parameterService.getLong(
                ParameterConstants.CACHE_TIMEOUT_CHANNEL_IN_MS, 60000);
        Map<String, Channel> channels = channelsCache;
        if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs
                || channels == null || refreshCache) {
            synchronized (this) {
                channels = channelsCache;
                if (System.currentTimeMillis() - channelCacheTime >= channelCacheTimeoutInMs
                        || channels == null || refreshCache) {
                    channels = new HashMap<String, Channel>();
                    List<Channel> list = sqlTemplate.query(getSql("selectChannelsSql"),
                            new ISqlRowMapper<Channel>() {
                                public Channel mapRow(Row row) {
                                    Channel channel = new Channel();
                                    channel.setChannelId(row.getString("channel_id"));
                                    channel.setProcessingOrder(row.getInt("processing_order"));
                                    channel.setMaxBatchSize(row.getInt("max_batch_size"));
                                    channel.setEnabled(row.getBoolean("enabled"));
                                    channel.setMaxBatchToSend(row.getInt("max_batch_to_send"));
                                    channel.setMaxDataToRoute(row.getInt("max_data_to_route"));
                                    channel.setUseOldDataToRoute(row
                                            .getBoolean("use_old_data_to_route"));
                                    channel.setUseRowDataToRoute(row
                                            .getBoolean("use_row_data_to_route"));
                                    channel.setUsePkDataToRoute(row
                                            .getBoolean("use_pk_data_to_route"));
                                    channel.setContainsBigLob(row.getBoolean("contains_big_lob"));
                                    channel.setBatchAlgorithm(row.getString("batch_algorithm"));
                                    channel.setExtractPeriodMillis(row
                                            .getLong("extract_period_millis"));
                                    channel.setDataLoaderType(row.getString("data_loader_type"));
                                    channel.setCreateTime(row.getDateTime("create_time"));
                                    channel.setLastUpdateBy(row.getString("last_update_by"));
                                    channel.setLastUpdateTime(row.getDateTime("last_update_time"));
                                    channel.setReloadFlag(row.getBoolean("reload_flag"));
                                    channel.setFileSyncFlag(row.getBoolean("file_sync_flag"));
                                    return channel;
                                }
                            });
                    for (Channel channel : list) {
                        channels.put(channel.getChannelId(), channel);
                    }
                    channelsCache = channels;
                    channelCacheTime = System.currentTimeMillis();
                }
            }
        }

        return channels;
    }

    public Channel getChannel(String channelId) {
        NodeChannel nodeChannel = getNodeChannel(channelId, false);
        if (nodeChannel != null) {
            return nodeChannel.getChannel();
        } else {
            return null;
        }
    }

    public ChannelMap getSuspendIgnoreChannelLists() {
        return getSuspendIgnoreChannelLists(nodeService.findIdentityNodeId());

    }

    class NodeGroupChannelWindowMapper implements ISqlRowMapper<NodeGroupChannelWindow> {
        public NodeGroupChannelWindow mapRow(Row row) {
            NodeGroupChannelWindow window = new NodeGroupChannelWindow();
            window.setNodeGroupId(row.getString("node_group_id"));
            window.setChannelId(row.getString("channel_id"));
            window.setStartTime(row.getTime("start_time"));
            window.setEndTime(row.getTime("end_time"));
            window.setEnabled(row.getBoolean("enabled"));
            return window;
        }
    }

    class NodeGroupLinkMapper implements ISqlRowMapper<NodeGroupLink> {
        public NodeGroupLink mapRow(Row row) {
            NodeGroupLink link = new NodeGroupLink();
            link.setSourceNodeGroupId(row.getString("source_node_group_id"));
            link.setTargetNodeGroupId(row.getString("target_node_group_id"));
            link.setDataEventAction(NodeGroupLinkAction.fromCode(row.getString("data_event_action")));
            link.setSyncConfigEnabled(row.getBoolean("sync_config_enabled"));
            link.setCreateTime(row.getDateTime("create_time"));
            link.setLastUpdateBy(row.getString("last_update_by"));
            link.setLastUpdateTime(row.getDateTime("last_update_time"));
            return link;
        }
    }

    class NodeGroupMapper implements ISqlRowMapper<NodeGroup> {
        public NodeGroup mapRow(Row row) {
            NodeGroup group = new NodeGroup();
            group.setNodeGroupId(row.getString("node_group_id"));
            group.setDescription(row.getString("description"));
            group.setCreateTime(row.getDateTime("create_time"));
            group.setLastUpdateBy(row.getString("last_update_by"));
            group.setLastUpdateTime(row.getDateTime("last_update_time"));
            return group;
        }
    }

    public Map<String, String> getRegistrationRedirectMap() {
        return this.sqlTemplate.queryForMap(getSql("getRegistrationRedirectSql"),
                "registrant_external_id", "registration_node_id");
    }
    
    @Override
    public void updateLastExtractTime(NodeChannel channel) {
        sqlTemplate.update(getSql("updateNodeChannelLastExtractTime"), channel.getLastExtractTime(), channel.getChannelId(), channel.getNodeId());
    }

}
