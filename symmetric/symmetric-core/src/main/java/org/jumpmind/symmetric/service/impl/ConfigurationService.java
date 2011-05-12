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
 * under the License.  */
package org.jumpmind.symmetric.service.impl;

import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.io.DatabaseIO;
import org.jumpmind.symmetric.ddl.model.Database;
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
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * @see IConfigurationService
 */
public class ConfigurationService extends AbstractService implements IConfigurationService {

    private INodeService nodeService;

    private Map<String, List<NodeChannel>> nodeChannelCache;

    private long nodeChannelCacheTime;

    private List<Channel> defaultChannels;

    public ConfigurationService() {
    }
    
    public void saveNodeGroupLink(NodeGroupLink link) {        
        if (!doesNodeGroupExist(link.getSourceNodeGroupId())) {
            saveNodeGroup(new NodeGroup(link.getSourceNodeGroupId()));
        }
        
        if (!doesNodeGroupExist(link.getTargetNodeGroupId())) {
            saveNodeGroup(new NodeGroup(link.getTargetNodeGroupId()));
        }
        
        if (jdbcTemplate.update(getSql("updateNodeGroupLinkSql"), link.getDataEventAction().name(), link.getSourceNodeGroupId(), link.getTargetNodeGroupId()) == 0) {
            jdbcTemplate.update(getSql("insertNodeGroupLinkSql"), link.getDataEventAction().name(), link.getSourceNodeGroupId(), link.getTargetNodeGroupId());
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
        if (jdbcTemplate.update(getSql("updateNodeGroupSql"), group.getDescription(), group.getNodeGroupId()) == 0) {
            jdbcTemplate.update(getSql("insertNodeGroupSql"), group.getDescription(), group.getNodeGroupId());
        }
    }
    
    public void deleteNodeGroup(String nodeGroupId) {
        jdbcTemplate.update(getSql("deleteNodeGroupSql"), nodeGroupId);
    }
    
    public void deleteNodeGroupLink(NodeGroupLink link) {
        jdbcTemplate.update(getSql("deleteNodeGroupLinkSql"), link.getSourceNodeGroupId(), link.getTargetNodeGroupId());
    }   
    
    public List<NodeGroup> getNodeGroups() {        
        return jdbcTemplate.query(getSql("selectNodeGroupsSql"), new NodeGroupMapper());
    }

    public List<NodeGroupLink> getNodeGroupLinks() {
        return jdbcTemplate.query(getSql("groupsLinksSql"), new NodeGroupLinkMapper());
    }

    public List<NodeGroupLink> getNodeGroupLinksFor(String sourceNodeGroupId) {
        return jdbcTemplate.query(getSql("groupsLinksForSql"), new Object[] { sourceNodeGroupId },
                new NodeGroupLinkMapper());
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
        return jdbcTemplate.queryForInt(getSql("isChannelInUseSql"), channelId) > 0;
    }

    public void saveChannel(Channel channel, boolean reloadChannels) {
        if (0 == jdbcTemplate.update(getSql("updateChannelSql"), new Object[] {
                channel.getProcessingOrder(), channel.getMaxBatchSize(),
                channel.getMaxBatchToSend(), channel.getMaxDataToRoute(),
                channel.isUseOldDataToRoute() ? 1 : 0, channel.isUseRowDataToRoute() ? 1 : 0,
                channel.isUsePkDataToRoute() ? 1 : 0,
                channel.isContainsBigLob() ? 1 : 0,
                channel.isEnabled() ? 1 : 0, channel.getBatchAlgorithm(),
                channel.getExtractPeriodMillis(), channel.getChannelId() })) {
            jdbcTemplate.update(getSql("insertChannelSql"), new Object[] { channel.getChannelId(),
                    channel.getProcessingOrder(), channel.getMaxBatchSize(),
                    channel.getMaxBatchToSend(), channel.getMaxDataToRoute(),
                    channel.isUseOldDataToRoute() ? 1 : 0, channel.isUseRowDataToRoute() ? 1 : 0,
                    channel.isUsePkDataToRoute() ? 1 : 0,
                    channel.isContainsBigLob() ? 1 : 0,
                    channel.isEnabled() ? 1 : 0, channel.getBatchAlgorithm(),
                    channel.getExtractPeriodMillis() });
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
                nodeChannel.isSuspendEnabled() ? 1 : 0, nodeChannel.isIgnoreEnabled() ? 1 : 0,
                nodeChannel.getLastExtractedTime(), nodeChannel.getNodeId(),
                nodeChannel.getChannelId() })) {
            jdbcTemplate.update(getSql("insertNodeChannelControlSql"), new Object[] {
                    nodeChannel.getNodeId(), nodeChannel.getChannelId(),
                    nodeChannel.isSuspendEnabled() ? 1 : 0, nodeChannel.isIgnoreEnabled() ? 1 : 0,
                    nodeChannel.getLastExtractedTime() });
        }
        if (reloadChannels) {
            reloadChannels();
        }
    }

    public void deleteChannel(Channel channel) {
        jdbcTemplate.update(getSql("deleteNodeChannelSql"), new Object[] { channel.getChannelId() });
        jdbcTemplate.update(getSql("deleteChannelSql"), new Object[] { channel.getChannelId() });
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
        List<NodeChannel> nodeChannels = nodeChannelCache != null ? nodeChannelCache.get(nodeId) : null;
        if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                || nodeChannels == null) {
            synchronized (this) {
                if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                        || nodeChannelCache == null || nodeChannelCache.get(nodeId) == null || nodeChannels == null) {
                    if (System.currentTimeMillis() - nodeChannelCacheTime >= channelCacheTimeoutInMs
                            || nodeChannelCache == null) {
                        nodeChannelCache = new HashMap<String, List<NodeChannel>>();
                        nodeChannelCacheTime = System.currentTimeMillis();
                    }
                    nodeChannels = jdbcTemplate.query(getSql("selectChannelsSql"),
                            new Object[] { nodeId }, new RowMapper<NodeChannel>() {
                        public NodeChannel mapRow(java.sql.ResultSet rs, int arg1)
                                throws SQLException {
                            NodeChannel nodeChannel = new NodeChannel();
                            nodeChannel.setChannelId(rs.getString(1));
                            nodeChannel.setNodeId(nodeId);
                            // note that 2 is intentionally missing here.                            
                            nodeChannel.setIgnoreEnabled(isSet(rs.getObject(3)));
                            nodeChannel.setSuspendEnabled(isSet(rs.getObject(4)));
                            nodeChannel.setProcessingOrder(rs.getInt(5));
                            nodeChannel.setMaxBatchSize(rs.getInt(6));
                            nodeChannel.setEnabled(rs.getBoolean(7));
                            nodeChannel.setMaxBatchToSend(rs.getInt(8));
                            nodeChannel.setMaxDataToRoute(rs.getInt(9));
                            nodeChannel.setUseOldDataToRoute(rs.getBoolean(10));
                            nodeChannel.setUseRowDataToRoute(rs.getBoolean(11));
                            nodeChannel.setUsePkDataToRoute(rs.getBoolean(12));
                            nodeChannel.setContainsBigLobs(rs.getBoolean(13));
                            nodeChannel.setBatchAlgorithm(rs.getString(14));
                            nodeChannel.setLastExtractedTime(rs.getTimestamp(15));
                            nodeChannel.setExtractPeriodMillis(rs.getLong(16));
                            return nodeChannel;
                        };
                    });
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

                jdbcTemplate.query(getSql("selectNodeChannelControlLastExtractTimeSql"),
                        new Object[] { nodeId }, new ResultSetExtractor<Object>() {
                            public Object extractData(ResultSet rs) throws SQLException,
                                    DataAccessException {
                                if (rs.next()) {
                                    String channelId = rs.getString(1);
                                    Date extractTime = rs.getTimestamp(2);
                                    nodeChannelsMap.get(channelId)
                                            .setLastExtractedTime(extractTime);
                                }
                                return null;
                            };
                        });

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
        String code = (String) jdbcTemplate.queryForObject(getSql("selectDataEventActionsByIdSql"),
                new Object[] { sourceGroupId, targetGroupId }, String.class);

        return NodeGroupLinkAction.fromCode(code);
    }

    public void autoConfigDatabase(boolean force) {
        if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE) || force) {
            log.info("SymmetricDSDatabaseInitializing");
            dbDialect.initTablesAndFunctions();
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
            List<NodeChannel> channels = getNodeChannels(false);
            for (Channel defaultChannel : defaultChannels) {
                if (!defaultChannel.isInList(channels)) {
                    log.info("ChannelAutoConfiguring", defaultChannel.getChannelId());
                    saveChannel(defaultChannel, true);
                } else {
                    log.info("ChannelExists", defaultChannel.getChannelId());
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

        if (node == null && StringUtils.isBlank(parameterService.getRegistrationUrl())
                && parameterService.is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false)) {
            log.info("AutoConfigRegistrationService");
            String nodeGroupId = parameterService.getNodeGroupId();
            String nodeId = parameterService.getExternalId();
            try {
                nodeService.insertNode(nodeId, nodeGroupId, nodeId, nodeId);
            } catch (DataIntegrityViolationException ex) {
                log.warn("AutoConfigNodeIdAlreadyExists", nodeId);
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
                    log.info("DatabaseSchemaBuilding", xml);
                    Database database = new DatabaseIO().read(new InputStreamReader(fileUrl
                            .openStream()));
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
                new SqlScript(fileUrl, dataSource, true).execute();
                loaded = true;
            }
        }
        return loaded;
    }

    public List<NodeGroupChannelWindow> getNodeGroupChannelWindows(String nodeGroupId,
            String channelId) {
        return (List<NodeGroupChannelWindow>) jdbcTemplate.query(
                getSql("selectNodeGroupChannelWindowSql"), new Object[] { nodeGroupId, channelId },
                new NodeGroupChannelWindowMapper());
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
            NodeGroupLink link = new NodeGroupLink();
            link.setSourceNodeGroupId(rs.getString(1));
            link.setTargetNodeGroupId(rs.getString(2));
            link.setDataEventAction(NodeGroupLinkAction.fromCode(rs.getString(3)));
            return link;
        }
    }
    
    class NodeGroupMapper implements RowMapper<NodeGroup> {
        public NodeGroup mapRow(ResultSet rs, int num) throws SQLException {
            NodeGroup group = new NodeGroup();
            group.setNodeGroupId(rs.getString(1));
            group.setDescription(rs.getString(2));
            return group;
        }
    }

}