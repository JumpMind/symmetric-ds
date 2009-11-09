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

import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ChannelMap;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.transaction.annotation.Transactional;

public class ConfigurationService extends AbstractService implements IConfigurationService {

    private INodeService nodeService;

    private static final long MAX_NODE_CHANNEL_CACHE_TIME = 60000;

    private Map<String, List<NodeChannel>> nodeChannelCache;

    private long nodeChannelCacheTime;

    private List<Channel> defaultChannels;

    public ConfigurationService() {
    }

    public List<NodeGroupLink> getGroupLinks() {
        return jdbcTemplate.query(getSql("groupsLinksSql"), new NodeGroupLinkMapper());
    }

    public List<NodeGroupLink> getGroupLinksFor(String nodeGroupId) {
        return jdbcTemplate.query(getSql("groupsLinksForSql"), new Object[] { nodeGroupId }, new NodeGroupLinkMapper());
    }

    public void saveChannel(Channel channel, boolean reloadChannels) {
        if (0 == jdbcTemplate.update(getSql("updateChannelSql"), new Object[] { channel.getProcessingOrder(),
                channel.getMaxBatchSize(), channel.getMaxBatchToSend(), channel.isEnabled() ? 1 : 0,
                channel.getBatchAlgorithm(), channel.getExtractPeriodMillis(), channel.getId() })) {
            jdbcTemplate.update(getSql("insertChannelSql"), new Object[] { channel.getId(),
                    channel.getProcessingOrder(), channel.getMaxBatchSize(), channel.getMaxBatchToSend(),
                    channel.isEnabled() ? 1 : 0, channel.getBatchAlgorithm(), channel.getExtractPeriodMillis() });
        }
        if (reloadChannels) {
            reloadChannels();
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
        boolean loaded = false;
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
                                    nodeChannel.setExtractPeriodMillis(rs.getLong(11));

                                    return nodeChannel;
                                };
                            }));
                    loaded = true;
                }
            }
        }

        if (!loaded) {

            // need to read last extracted time from database regardless of
            // whether we used the cache or not.
            // locate the nodes in the cache, and update it.

            List<NodeChannel> nodeChannels = nodeChannelCache.get(nodeId);
            final Map<String, NodeChannel> nodeChannelsMap = new HashMap<String, NodeChannel>();

            for (NodeChannel nc : nodeChannels) {
                nodeChannelsMap.put(nc.getId(), nc);
            }

            jdbcTemplate.query(getSql("selectNodeChannelControlLastExtractTimeSql"), new Object[] { nodeId },
                    new ResultSetExtractor() {
                        public Object extractData(ResultSet rs) throws SQLException, DataAccessException {
                            if (rs.next()) {
                                String channelId = rs.getString(1);
                                Date extractTime = rs.getTimestamp(2);
                                nodeChannelsMap.get(channelId).setLastExtractedTime(extractTime);
                            }
                            return null;
                        };
                    });
        }

        return nodeChannelCache.get(nodeId);
    }

    public void reloadChannels() {
        synchronized (this) {
            nodeChannelCache = null;
        }
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

    @Transactional
    public void autoConfigDatabase(boolean force) {
        if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE) || force) {
            log.info("SymmetricDSDatabaseInitializing");
            dbDialect.initSyncDb();
            autoConfigChannels();
            autoConfigRegistrationServer();
            parameterService.rereadParameters();
            log.info("SymmetricDSDatabaseInitialized");
        } else {
            log.info("SymmetricDSDatabaseNotAutoConfig");
        }
    }

    protected void autoConfigChannels() {
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
    }

    protected void autoConfigRegistrationServer() {
        Node node = nodeService.findIdentity();
        
        if (node == null) {
            buildTablesFromDdlUtilXmlIfProvided();
            loadFromScriptIfProvided();
        }
        
        if (node == null && StringUtils.isBlank(parameterService.getRegistrationUrl()) &&
                parameterService.is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND)) {
            log.info("AutoConfigRegistrationService");
            String nodeGroupId = parameterService.getNodeGroupId();
            String nodeId = parameterService.getExternalId();
            nodeService.insertNode(nodeId, nodeGroupId, nodeId, nodeId);            
            nodeService.insertNodeIdentity(nodeId);
            node = nodeService.findIdentity();
            node.setSyncUrl(parameterService.getSyncUrl());
            node.setSyncEnabled(true);
            node.setHeartbeatTime(new Date());
            nodeService.updateNode(node);
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId, true);
            nodeSecurity.setInitialLoadTime(new Date());
            nodeSecurity.setRegistrationTime(new Date());
            nodeSecurity.setInitialLoadEnabled(false);
            nodeSecurity.setRegistrationEnabled(false);
            nodeService.updateNodeSecurity(nodeSecurity);
        }
    }
    
    private boolean buildTablesFromDdlUtilXmlIfProvided() {
        boolean loaded = false;
        String xml = parameterService.getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML);
        if (!StringUtils.isBlank(xml)) {
            File file = new File(xml);
            URL fileUrl = null;
            if (file.isFile()) {
                try {
                    fileUrl = file.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            } else {
                fileUrl = getClass().getResource(xml);
            }

            if (fileUrl != null) {
                try {
                    log.info("DatabaseSchemaBuilding", xml);
                    Database database = new DatabaseIO().read(new InputStreamReader(fileUrl.openStream()));
                    Platform platform = dbDialect.getPlatform();
                    platform.createTables(database, false, true);
                    loaded = true;
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
        return loaded;
    }

    /**
     * Give the end user the option to provide a script that will load a
     * registration server with an initial SymmetricDS setup.
     * 
     * Look first on the file system, then in the classpath for the SQL file.
     * 
     * @return true if the script was executed
     */
    private boolean loadFromScriptIfProvided() {
        boolean loaded = false;
        String sqlScript = parameterService.getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT);
        if (!StringUtils.isBlank(sqlScript)) {
            File file = new File(sqlScript);
            URL fileUrl = null;
            if (file.isFile()) {
                try {
                    fileUrl = file.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            } else {
                fileUrl = getClass().getResource(sqlScript);
                if (fileUrl == null) {
                    fileUrl = Thread.currentThread().getContextClassLoader().getResource(sqlScript);
                }
            }

            if (fileUrl != null) {
                log.info("ScriptRunning", sqlScript);
                new SqlScript(fileUrl, dbDialect.getJdbcTemplate().getDataSource(), true).execute();
                loaded = true;
            }
        }
        return loaded;
    }

    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String nodeGroupId, String channelId) {
        return (List<NodeGroupChannelWindow>) jdbcTemplate.query(getSql("selectNodeGroupChannelWindowSql"),
                new Object[] { nodeGroupId, channelId }, new NodeGroupChannelWindowMapper());
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

    class NodeGroupChannelWindowMapper implements RowMapper<NodeGroupChannelWindow> {
        public NodeGroupChannelWindow mapRow(ResultSet rs, int rowNum) throws SQLException {
            NodeGroupChannelWindow window = new NodeGroupChannelWindow();
            window.setNodeGroupId(rs.getString(1));
            window.setChannelId(rs.getString(2));
            window.setStartTime(rs.getTime(3));
            window.setEndTime(rs.getTime(4));
            window.setEnabled(rs.getBoolean(5));
            return window;
        }
    }

    class NodeGroupLinkMapper implements RowMapper<NodeGroupLink> {
        public NodeGroupLink mapRow(ResultSet rs, int num) throws SQLException {
            NodeGroupLink node_groupTarget = new NodeGroupLink();
            node_groupTarget.setSourceGroupId(rs.getString(1));
            node_groupTarget.setTargetGroupId(rs.getString(2));
            node_groupTarget.setDataEventAction(DataEventAction.fromCode(rs.getString(3)));
            return node_groupTarget;
        }
    }

}
