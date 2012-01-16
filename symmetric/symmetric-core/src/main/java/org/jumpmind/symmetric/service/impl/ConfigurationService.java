/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.sql.UniqueKeyException;
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
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * @see IConfigurationService
 */
public class ConfigurationService extends AbstractService implements IConfigurationService {

    private INodeService nodeService;

    private Map<String, List<NodeChannel>> nodeChannelCache;

    private long nodeChannelCacheTime;

    private List<Channel> defaultChannels;

    public ConfigurationService(IParameterService parameterService, ISymmetricDialect dialect,
            INodeService nodeService) {
        super(parameterService, dialect);
        this.nodeService = nodeService;
        this.defaultChannels = new ArrayList<Channel>();
        this.defaultChannels.add(new Channel(Constants.CHANNEL_CONFIG, 0, 100, 100, true, 0));
        this.defaultChannels.add(new Channel(Constants.CHANNEL_RELOAD, 1, 1, 1, true, 0));
        this.defaultChannels.add(new Channel(Constants.CHANNEL_DEFAULT, 99999, 1000, 100, true, 0));
    }

    public void saveNodeGroupLink(NodeGroupLink link) {
        if (!doesNodeGroupExist(link.getSourceNodeGroupId())) {
            saveNodeGroup(new NodeGroup(link.getSourceNodeGroupId()));
        }

        if (!doesNodeGroupExist(link.getTargetNodeGroupId())) {
            saveNodeGroup(new NodeGroup(link.getTargetNodeGroupId()));
        }

        if (sqlTemplate.update(getSql("updateNodeGroupLinkSql"), link.getDataEventAction().name(),
                link.getSourceNodeGroupId(), link.getTargetNodeGroupId()) == 0) {
            sqlTemplate.update(getSql("insertNodeGroupLinkSql"), link.getDataEventAction().name(),
                    link.getSourceNodeGroupId(), link.getTargetNodeGroupId());
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
        if (sqlTemplate.update(getSql("updateNodeGroupSql"), group.getDescription(),
                group.getNodeGroupId()) == 0) {
            sqlTemplate.update(getSql("insertNodeGroupSql"), group.getDescription(),
                    group.getNodeGroupId());
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

    public List<NodeGroupLink> getNodeGroupLinks() {
        return sqlTemplate.query(getSql("groupsLinksSql"), new NodeGroupLinkMapper());
    }

    public List<NodeGroupLink> getNodeGroupLinksFor(String sourceNodeGroupId) {
        return sqlTemplate.query(getSql("groupsLinksForSql"), new NodeGroupLinkMapper(),
                sourceNodeGroupId);
    }

    public NodeGroupLink getNodeGroupLinkFor(String sourceNodeGroupId, String targetNodeGroupId) {
        List<NodeGroupLink> links = getNodeGroupLinksFor(sourceNodeGroupId);
        Iterator<NodeGroupLink> it = links.iterator();
        while (it.hasNext()) {
            NodeGroupLink nodeGroupLink = (NodeGroupLink) it.next();
            if (!nodeGroupLink.getTargetNodeGroupId().equals(targetNodeGroupId)) {
                it.remove();
            }
        }
        return links.size() > 0 ? links.get(0) : null;
    }

    public boolean isChannelInUse(String channelId) {
        return sqlTemplate.queryForInt(getSql("isChannelInUseSql"), channelId) > 0;
    }

    public void saveChannel(Channel channel, boolean reloadChannels) {
        if (0 == sqlTemplate.update(
                getSql("updateChannelSql"),
                new Object[] { channel.getProcessingOrder(), channel.getMaxBatchSize(),
                        channel.getMaxBatchToSend(), channel.getMaxDataToRoute(),
                        channel.isUseOldDataToRoute() ? 1 : 0,
                        channel.isUseRowDataToRoute() ? 1 : 0,
                        channel.isUsePkDataToRoute() ? 1 : 0, channel.isContainsBigLob() ? 1 : 0,
                        channel.isEnabled() ? 1 : 0, channel.getBatchAlgorithm(),
                        channel.getExtractPeriodMillis(), channel.getChannelId() })) {
            sqlTemplate.update(
                    getSql("insertChannelSql"),
                    new Object[] { channel.getChannelId(), channel.getProcessingOrder(),
                            channel.getMaxBatchSize(), channel.getMaxBatchToSend(),
                            channel.getMaxDataToRoute(), channel.isUseOldDataToRoute() ? 1 : 0,
                            channel.isUseRowDataToRoute() ? 1 : 0,
                            channel.isUsePkDataToRoute() ? 1 : 0,
                            channel.isContainsBigLob() ? 1 : 0, channel.isEnabled() ? 1 : 0,
                            channel.getBatchAlgorithm(), channel.getExtractPeriodMillis() });
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
        if (0 == sqlTemplate.update(
                getSql("updateNodeChannelControlSql"),
                new Object[] { nodeChannel.isSuspendEnabled() ? 1 : 0,
                        nodeChannel.isIgnoreEnabled() ? 1 : 0, nodeChannel.getLastExtractTime(),
                        nodeChannel.getNodeId(), nodeChannel.getChannelId() })) {
            sqlTemplate.update(
                    getSql("insertNodeChannelControlSql"),
                    new Object[] { nodeChannel.getNodeId(), nodeChannel.getChannelId(),
                            nodeChannel.isSuspendEnabled() ? 1 : 0,
                            nodeChannel.isIgnoreEnabled() ? 1 : 0,
                            nodeChannel.getLastExtractTime() });
        }
        if (reloadChannels) {
            reloadChannels();
        }
    }

    public void deleteChannel(Channel channel) {
        sqlTemplate.update(getSql("deleteNodeChannelSql"), new Object[] { channel.getChannelId() });
        sqlTemplate.update(getSql("deleteChannelSql"), new Object[] { channel.getChannelId() });
        reloadChannels();
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
                    nodeChannels = sqlTemplate.query(getSql("selectChannelsSql"),
                            new ISqlRowMapper<NodeChannel>() {
                                public NodeChannel mapRow(Row row) {
                                    NodeChannel nodeChannel = new NodeChannel();
                                    nodeChannel.setChannelId(row.getString("channel_id"));
                                    nodeChannel.setNodeId(nodeId);
                                    // note that 2 is intentionally missing
                                    // here.
                                    nodeChannel.setIgnoreEnabled(row.getBoolean("ignore_enabled"));
                                    nodeChannel.setSuspendEnabled(row.getBoolean("suspend_enabled"));
                                    nodeChannel.setProcessingOrder(row.getInt("processing_order"));
                                    nodeChannel.setMaxBatchSize(row.getInt("max_batch_size"));
                                    nodeChannel.setEnabled(row.getBoolean("enabled"));
                                    nodeChannel.setMaxBatchToSend(row.getInt("max_batch_to_send"));
                                    nodeChannel.setMaxDataToRoute(row.getInt("max_data_to_route"));
                                    nodeChannel.setUseOldDataToRoute(row
                                            .getBoolean("use_old_data_to_route"));
                                    nodeChannel.setUseRowDataToRoute(row
                                            .getBoolean("use_row_data_to_route"));
                                    nodeChannel.setUsePkDataToRoute(row
                                            .getBoolean("use_pk_data_to_route"));
                                    nodeChannel.setContainsBigLobs(row
                                            .getBoolean("contains_big_lob"));
                                    nodeChannel.setBatchAlgorithm(row.getString("batch_algorithm"));
                                    nodeChannel.setLastExtractTime(row
                                            .getDateTime("last_extract_time"));
                                    nodeChannel.setExtractPeriodMillis(row
                                            .getLong("extract_period_millis"));
                                    return nodeChannel;
                                };
                            }, nodeId);
                    nodeChannelCache.put(nodeId, nodeChannels);
                    loaded = true;
                }
            }
        }

        if (!loaded && refreshExtractMillis) {
            // need to read last extracted time from database regardless of
            // whether we used the cache or not.
            // locate the nodes in the cache, and update it.
            final Map<String, NodeChannel> nodeChannelsMap = new HashMap<String, NodeChannel>();

            for (NodeChannel nc : nodeChannels) {
                nodeChannelsMap.put(nc.getChannelId(), nc);
            }

            sqlTemplate.query(getSql("selectNodeChannelControlLastExtractTimeSql"),
                    new ISqlRowMapper<Object>() {
                        public Object mapRow(Row row) {
                            String channelId = row.getString("channel_id");
                            Date extractTime = row.getDateTime("last_extract_time");
                            nodeChannelsMap.get(channelId).setLastExtractTime(extractTime);
                            return null;
                        };
                    }, nodeId);

        }

        return nodeChannels;
    }

    public void reloadChannels() {
        synchronized (this) {
            nodeChannelCache = null;
        }
    }

    public NodeGroupLinkAction getDataEventActionByGroupLinkId(String sourceGroupId,
            String targetGroupId) {
        String code = (String) sqlTemplate.queryForObject(getSql("selectDataEventActionsByIdSql"),
                String.class, sourceGroupId, targetGroupId);
        return NodeGroupLinkAction.fromCode(code);
    }

    public void autoConfigDatabase(boolean force) {
        if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE) || force) {
            log.info("Initializing SymmetricDS database.");
            symmetricDialect.initTablesAndFunctions();
            autoConfigChannels();
            autoConfigRegistrationServer();
            parameterService.rereadParameters();
            log.info("Done initializing SymmetricDS database.");
        } else {
            log.info("SymmetricDS is not configured to auto-create the database.");
        }
    }

    protected void autoConfigChannels() {
        if (defaultChannels != null) {
            reloadChannels();
            List<NodeChannel> channels = getNodeChannels(false);
            for (Channel defaultChannel : defaultChannels) {
                if (!defaultChannel.isInList(channels)) {
                    log.info("Auto-configuring {} channel.", defaultChannel.getChannelId());
                    saveChannel(defaultChannel, true);
                } else {
                    log.debug("No need to create channel {}.  It already exists.", defaultChannel.getChannelId());
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

        node = nodeService.findIdentity();

        if (node == null && StringUtils.isBlank(parameterService.getRegistrationUrl())
                && parameterService.is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false)) {
            log.info("Inserting rows for node, security, identity and group for registration server");
            String nodeGroupId = parameterService.getNodeGroupId();
            String nodeId = parameterService.getExternalId();
            try {
                nodeService.insertNode(nodeId, nodeGroupId, nodeId, nodeId);
            } catch (UniqueKeyException ex) {
                log.warn("Not inserting node row for {} because it already exists", nodeId);
            }
            nodeService.insertNodeIdentity(nodeId);
            node = nodeService.findIdentity();
            node.setSyncUrl(parameterService.getSyncUrl());
            node.setSyncEnabled(true);
            node.setHeartbeatTime(new Date());
            nodeService.updateNode(node);
            nodeService.insertNodeGroup(node.getNodeGroupId(), null);
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
        String xml = parameterService
                .getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML);
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
                    log.info("Building database schema from: {}", xml);
                    Database database = new DatabaseIO().read(new InputStreamReader(fileUrl
                            .openStream()));
                    IDatabasePlatform platform = symmetricDialect.getPlatform();
                    platform.createDatabase(database, true, true);
                    loaded = true;
                } catch (Exception e) {
                    log.error(e.getMessage(),e);
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
        String sqlScript = parameterService
                .getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT);
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
                new SqlScript(fileUrl, symmetricDialect.getPlatform().getSqlTemplate(), true,
                        SqlScript.QUERY_ENDS, getSymmetricDialect().getPlatform()
                                .getSqlScriptReplacementTokens()).execute();
                loaded = true;
            }
        }
        return loaded;
    }

    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String nodeGroupId,
            String channelId) {
        return (List<NodeGroupChannelWindow>) sqlTemplate.query(
                getSql("selectNodeGroupChannelWindowSql"), new NodeGroupChannelWindowMapper(),
                nodeGroupId, channelId);
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

    @Override
    protected AbstractSqlMap createSqlMap() {
        return new ConfigurationServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens());
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
            return link;
        }
    }

    class NodeGroupMapper implements ISqlRowMapper<NodeGroup> {
        public NodeGroup mapRow(Row row) {
            NodeGroup group = new NodeGroup();
            group.setNodeGroupId(row.getString("node_group_id"));
            group.setDescription(row.getString("description"));
            return group;
        }
    }

    public Map<String, String> getRegistrationRedirectMap() {
        return this.sqlTemplate.queryForMap(getSql("getRegistrationRedirectSql"),
                "registrant_external_id", "registration_node_id");
    }

}
