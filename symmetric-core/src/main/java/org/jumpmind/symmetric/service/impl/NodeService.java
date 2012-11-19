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

import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.UniqueKeyException;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.INodeIdCreator;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.DefaultNodeIdCreator;
import org.jumpmind.util.AppUtils;

/**
 * @see INodeService
 */
public class NodeService extends AbstractService implements INodeService {

    private Node cachedNodeIdentity;

    private Map<String, NodeSecurity> securityCache;

    private long securityCacheTime;

    private INodeIdCreator nodeIdCreator;

    private INodePasswordFilter nodePasswordFilter;

    private NodeHost nodeHostForCurrentNode = null;

    private List<IOfflineServerListener> offlineServerListeners;

    public NodeService(IParameterService parameterService, ISymmetricDialect dialect) {
        super(parameterService, dialect);
        nodeIdCreator = new DefaultNodeIdCreator(parameterService, this);
        setSqlMap(new NodeServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public String findSymmetricVersion() {
        return (String) sqlTemplate.queryForObject(getSql("findSymmetricVersionSql"), String.class);
    }

    public String findIdentityNodeId() {
        Node node = findIdentity();
        return node != null ? node.getNodeId() : null;
    }

    public Collection<Node> findEnabledNodesFromNodeGroup(String nodeGroupId) {
        return sqlTemplate.query(getSql("selectNodePrefixSql", "findEnabledNodesFromNodeGroupSql"),
                new NodeRowMapper(), new Object[] { nodeGroupId });
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId) {
        return findNodesThatOriginatedFromNodeId(originalNodeId, true);
    }

    public Collection<Node> findNodesWithOpenRegistration() {
        return sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesWithOpenRegistrationSql"),
                new NodeRowMapper());
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId, boolean recursive) {
        Set<Node> all = new HashSet<Node>();
        List<Node> list = sqlTemplate.query(
                getSql("selectNodePrefixSql", "findNodesCreatedByMeSql"), new NodeRowMapper(),
                originalNodeId);
        if (list.size() > 0) {
            all.addAll(list);
            if (recursive) {
                for (Node node : list) {
                    all.addAll(findNodesThatOriginatedFromNodeId(node.getNodeId()));
                }
            }
        }
        return all;
    }

    /**
     * Lookup a node in the database, which contains information for syncing
     * with it.
     */
    public Node findNode(String id) {
        List<Node> list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodeSql"),
                new NodeRowMapper(), id);
        return (Node) getFirstEntry(list);
    }

    public Node findNodeByExternalId(String nodeGroupId, String externalId) {
        List<Node> list = sqlTemplate.query(
                getSql("selectNodePrefixSql", "findNodeByExternalIdSql"), new NodeRowMapper(),
                nodeGroupId, externalId);
        return (Node) getFirstEntry(list);
    }

    public void ignoreNodeChannelForExternalId(boolean enabled, String channelId,
            String nodeGroupId, String externalId) {
        Node node = findNodeByExternalId(nodeGroupId, externalId);
        if (sqlTemplate.update(getSql("nodeChannelControlIgnoreSql"), new Object[] {
                enabled ? 1 : 0, node.getNodeId(), channelId }) == 0) {
            sqlTemplate.update(getSql("insertNodeChannelControlSql"),
                    new Object[] { node.getNodeId(), channelId, enabled ? 1 : 0, 0 });
        }
    }

    public boolean isRegistrationEnabled(String nodeId) {
        NodeSecurity nodeSecurity = findNodeSecurity(nodeId);
        if (nodeSecurity != null) {
            return nodeSecurity.isRegistrationEnabled();
        }
        return false;
    }

    /**
     * Lookup a node_security in the database, which contains private
     * information used to authenticate.
     */
    public NodeSecurity findNodeSecurity(String id) {
        return findNodeSecurity(id, false);
    }

    public List<NodeHost> findNodeHosts(String nodeId) {
        return sqlTemplate.query(getSql("selectNodeHostPrefixSql", "selectNodeHostByNodeIdSql"),
                new NodeHostRowMapper(), nodeId);
    }        

    public void updateNodeHostForCurrentNode() {
        if (nodeHostForCurrentNode == null) {
            nodeHostForCurrentNode = new NodeHost(findIdentityNodeId());
        }
        nodeHostForCurrentNode.refresh();
        Object[] params = new Object[] { nodeHostForCurrentNode.getIpAddress(),
                nodeHostForCurrentNode.getOsUser(), nodeHostForCurrentNode.getOsName(),
                nodeHostForCurrentNode.getOsArch(), nodeHostForCurrentNode.getOsVersion(),
                nodeHostForCurrentNode.getAvailableProcessors(),
                nodeHostForCurrentNode.getFreeMemoryBytes(),
                nodeHostForCurrentNode.getTotalMemoryBytes(),
                nodeHostForCurrentNode.getMaxMemoryBytes(),
                nodeHostForCurrentNode.getJavaVersion(), nodeHostForCurrentNode.getJavaVendor(),
                nodeHostForCurrentNode.getSymmetricVersion(),
                nodeHostForCurrentNode.getTimezoneOffset(),
                nodeHostForCurrentNode.getHeartbeatTime(),
                nodeHostForCurrentNode.getLastRestartTime(), nodeHostForCurrentNode.getNodeId(),
                nodeHostForCurrentNode.getHostName() };
        if (sqlTemplate.update(getSql("updateNodeHostSql"), params) == 0) {
            sqlTemplate.update(getSql("insertNodeHostSql"), params);
        }
    }

    public NodeSecurity findNodeSecurity(String nodeId, boolean createIfNotFound) {
        try {
            if (nodeId != null) {
                List<NodeSecurity> list = sqlTemplate.query(getSql("findNodeSecuritySql"),
                        new NodeSecurityRowMapper(), new Object[] { nodeId },
                        new int[] { Types.VARCHAR });
                NodeSecurity security = (NodeSecurity) getFirstEntry(list);
                if (security == null && createIfNotFound) {
                    insertNodeSecurity(nodeId);
                    security = findNodeSecurity(nodeId, false);
                }
                return security;
            } else {
                log.debug("A 'null' node id was passed into findNodeSecurity");
                return null;
            }
        } catch (UniqueKeyException ex) {
            log.error(
                    "Could not find a node security row for '{}'",
                    nodeId);
            throw ex;
        }
    }

    public void deleteNodeSecurity(String nodeId) {
        sqlTemplate.update(getSql("deleteNodeSecuritySql"), new Object[] { nodeId });
    }

    public void deleteNode(String nodeId) {
        if (StringUtils.isNotBlank(nodeId)) {
            if (nodeId.equals(findIdentityNodeId())) {
                sqlTemplate.update(getSql("deleteNodeIdentitySql"));
            }
            sqlTemplate.update(getSql("deleteNodeHostSql"), new Object[] { nodeId });
            sqlTemplate.update(getSql("deleteNodeSql"), new Object[] { nodeId });
        }
    }

    public void insertNodeSecurity(String id) {
        flushNodeAuthorizedCache();
        String password = nodeIdCreator.generatePassword(new Node(id, null, null));
        password = filterPasswordOnSaveIfNeeded(password);
        sqlTemplate.update(getSql("insertNodeSecuritySql"), new Object[] { id, password,
                findIdentity().getNodeId() });
    }

    public void insertNodeIdentity(String nodeId) {
        sqlTemplate.update(getSql("insertNodeIdentitySql"), nodeId);
    }

    public boolean deleteIdentity() {
        boolean successful = false;
        try {
            sqlTemplate.update(getSql("deleteNodeIdentitySql"));
            successful = true;
        } catch (SqlException ex) {
            log.info(ex.getMessage());
        } finally {
            cachedNodeIdentity = null;
        }
        return successful;
    }

    public void insertNodeGroup(String groupId, String description) {
        if (sqlTemplate.queryForInt(getSql("doesNodeGroupExistSql"), groupId) == 0) {
            sqlTemplate.update(getSql("insertNodeGroupSql"), description, groupId);
        }
    }
    
    public void save(Node node) {
        if (!updateNode(node)) {
            sqlTemplate.update(
                    getSql("insertNodeSql"),
                    new Object[] { node.getNodeGroupId(), node.getExternalId(), node.getDatabaseType(),
                            node.getDatabaseVersion(), node.getSchemaVersion(),
                            node.getSymmetricVersion(), node.getSyncUrl(), new Date(),
                            node.isSyncEnabled() ? 1 : 0, AppUtils.getTimezoneOffset(),
                            node.getBatchToSendCount(), node.getBatchInErrorCount(),
                            node.getCreatedAtNodeId(), node.getDeploymentType(), node.getNodeId() },
                    new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                            Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR });
        }
    }

    public boolean updateNode(Node node) {
        boolean updated = sqlTemplate.update(
                getSql("updateNodeSql"),
                new Object[] { node.getNodeGroupId(), node.getExternalId(), node.getDatabaseType(),
                        node.getDatabaseVersion(), node.getSchemaVersion(),
                        node.getSymmetricVersion(), node.getSyncUrl(), new Date(),
                        node.isSyncEnabled() ? 1 : 0,  AppUtils.getTimezoneOffset(),
                        node.getBatchToSendCount(), node.getBatchInErrorCount(),
                        node.getCreatedAtNodeId(), node.getDeploymentType(), node.getNodeId() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                        Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR }) == 1;
        return updated;
    }

    protected <T> T getFirstEntry(List<T> list) {
        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return null;
    }

    public Map<String, NodeSecurity> findAllNodeSecurity(boolean useCache) {
        long maxSecurityCacheTime = parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_NODE_SECURITY_IN_MS);
        Map<String, NodeSecurity> all = securityCache;
        if (all == null || System.currentTimeMillis() - securityCacheTime >= maxSecurityCacheTime
                || securityCacheTime == 0 || !useCache) {
            all = (Map<String, NodeSecurity>) sqlTemplate.queryForMap(
                    getSql("findAllNodeSecuritySql"), new NodeSecurityRowMapper(), "node_id");
            securityCache = all;
            securityCacheTime = System.currentTimeMillis();
        }
        return all;
    }

    /**
     * Check that the given node and password match in the node_security table.
     * A node must authenticate before it's allowed to sync data.
     */
    public boolean isNodeAuthorized(String nodeId, String password) {
        Map<String, NodeSecurity> nodeSecurities = findAllNodeSecurity(true);
        NodeSecurity nodeSecurity = nodeSecurities.get(nodeId);
        if (nodeSecurity != null
                && !nodeId.equals(findIdentityNodeId()) && ((nodeSecurity.getNodePassword() != null
                        && !nodeSecurity.getNodePassword().equals("") && nodeSecurity
                        .getNodePassword().equals(password)) || nodeSecurity
                            .isRegistrationEnabled())) {
            return true;
        }
        return false;
    }

    public void flushNodeAuthorizedCache() {
        securityCacheTime = 0;
    }

    public Node getCachedIdentity() {
        return cachedNodeIdentity;
    }

    public Node findIdentity() {
        return findIdentity(true);
    }

    public Node findIdentity(boolean useCache) {
        if (cachedNodeIdentity == null || useCache == false) {
            try {
                List<Node> list = sqlTemplate.query(
                        getSql("selectNodePrefixSql", "findNodeIdentitySql"), new NodeRowMapper());
                cachedNodeIdentity = (Node) getFirstEntry(list);
            } catch (SqlException ex) {
                log.warn("Failed to load the node identity because: {}.  Returning {}", ex.getMessage(), cachedNodeIdentity);
            }
        }
        return cachedNodeIdentity;
    }

    public List<Node> findNodesToPull() {
        return findSourceNodesFor(NodeGroupLinkAction.W);
    }

    public List<Node> findNodesToPushTo() {
        return findTargetNodesFor(NodeGroupLinkAction.P);
    }

    public List<Node> findSourceNodesFor(NodeGroupLinkAction eventAction) {
        Node node = findIdentity();
        if (node != null) {
            return sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesWhoTargetMeSql"),
                    new NodeRowMapper(), node.getNodeGroupId(), eventAction.name());
        } else {
            return Collections.emptyList();
        }
    }

    public List<Node> findTargetNodesFor(NodeGroupLinkAction eventAction) {
        Node node = findIdentity();
        if (node != null) {
            return sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesWhoITargetSql"),
                    new NodeRowMapper(), node.getNodeGroupId(), eventAction.name());
        } else {
            return Collections.emptyList();
        }
    }

    public List<String> findAllExternalIds() {
        return sqlTemplate.query(getSql("selectExternalIdsSql"), new StringMapper());
    }

    public List<Node> findAllNodes() {
        return sqlTemplate.query(getSql("selectNodePrefixSql"), new NodeRowMapper());
    }

    public Map<String, Node> findAllNodesAsMap() {
        List<Node> nodes = findAllNodes();
        Map<String, Node> nodeMap = new HashMap<String, Node>(nodes.size());
        for (Node node : nodes) {
            nodeMap.put(node.getNodeId(), node);
        }
        return nodeMap;
    }

    public NetworkedNode getRootNetworkedNode() {
        Map<String, Node> nodes = findAllNodesAsMap();
        Map<String, NetworkedNode> leaves = new HashMap<String, NetworkedNode>(nodes.size());
        NetworkedNode nodeLeaf = null;
        for (Node node : nodes.values()) {
            nodeLeaf = leaves.get(node.getNodeId());
            if (nodeLeaf == null) {
                nodeLeaf = new NetworkedNode(node);
                nodeLeaf.addParents(nodes, leaves);
                leaves.put(node.getNodeId(), nodeLeaf);
            }
        }
        
        NetworkedNode root = nodeLeaf.getRoot();
        root.setAllNetworkedNodes(leaves);
        return root;
    }
    
    public boolean updateNodeSecurity(NodeSecurity security) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            boolean updated = updateNodeSecurity(transaction, security);
            transaction.commit();
            return updated;
        } finally {
            close(transaction);
        }
    }

    public boolean updateNodeSecurity(ISqlTransaction transaction, NodeSecurity security) {
        flushNodeAuthorizedCache();
        security.setNodePassword(filterPasswordOnSaveIfNeeded(security.getNodePassword()));
        return transaction.prepareAndExecute(getSql("updateNodeSecuritySql"),
                new Object[] { security.getNodePassword(),
                        security.isRegistrationEnabled() ? 1 : 0, security.getRegistrationTime(),
                        security.isInitialLoadEnabled() ? 1 : 0, security.getInitialLoadTime(),
                        security.getCreatedAtNodeId(), security.getNodeId() }, new int[] {
                        Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP, Types.INTEGER,
                        Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR }) == 1;
    }
    
    public boolean setInitialLoadEnabled(ISqlTransaction transaction, String nodeId, boolean initialLoadEnabled) {
        NodeSecurity nodeSecurity = findNodeSecurity(nodeId, true);
        if (nodeSecurity != null) {
            nodeSecurity.setInitialLoadEnabled(initialLoadEnabled);
            if (initialLoadEnabled) {
                nodeSecurity.setInitialLoadTime(null);
            } else {
                nodeSecurity.setInitialLoadTime(new Date());
            }
            return updateNodeSecurity(transaction, nodeSecurity);
        }
        return false;        
    }

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            boolean updated = setInitialLoadEnabled(transaction, nodeId, initialLoadEnabled);
            transaction.commit();
            return updated;
        } finally {
            close(transaction);
        }
    }

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId) {
        return sqlTemplate.queryForInt(getSql("isNodeRegisteredSql"), new Object[] { nodeGroupId,
                externalId }) > 0;
    }

    public INodeIdCreator getNodeIdCreator() {
        return nodeIdCreator;
    }

    public void setNodeIdCreator(INodeIdCreator nodeIdGenerator) {
        this.nodeIdCreator = nodeIdGenerator;
    }

    public boolean isDataLoadCompleted() {
        return getNodeStatus() == NodeStatus.DATA_LOAD_COMPLETED;
    }

    public boolean isDataLoadStarted() {
        return getNodeStatus() == NodeStatus.DATA_LOAD_STARTED;
    }

    public boolean isRegistrationServer() {
        return StringUtils.isBlank(parameterService.getRegistrationUrl())
                || parameterService.getRegistrationUrl().equals(parameterService.getSyncUrl());
    }

    public NodeStatus getNodeStatus() {
        long ts = System.currentTimeMillis();
        try {
            class DataLoadStatus {
                int initialLoadEnabled;
                Date initialLoadTime;
            }

            List<DataLoadStatus> results = sqlTemplate.query(getSql("getDataLoadStatusSql"),
                    new ISqlRowMapper<DataLoadStatus>() {
                        public DataLoadStatus mapRow(Row rs) {
                            DataLoadStatus status = new DataLoadStatus();
                            status.initialLoadEnabled = rs.getInt("initial_load_enabled");
                            status.initialLoadTime = rs.getDateTime("initial_load_time");
                            return status;
                        }
                    });

            if (results.size() > 0) {
                DataLoadStatus status = results.get(0);
                if (status.initialLoadEnabled == 1) {
                    return NodeStatus.DATA_LOAD_STARTED;
                } else if (status.initialLoadTime != null) {
                    return NodeStatus.DATA_LOAD_COMPLETED;
                }
            }
            return NodeStatus.DATA_LOAD_NOT_STARTED;
        } catch (SqlException ex) {
            log.error("Could not query table after {} ms.  The status is unknown.",
                    (System.currentTimeMillis() - ts), ex);
            return NodeStatus.STATUS_UNKNOWN;
        }
    }

    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter) {
        this.nodePasswordFilter = nodePasswordFilter;
    }

    private String filterPasswordOnSaveIfNeeded(String password) {
        String s = password;
        if (nodePasswordFilter != null) {
            s = nodePasswordFilter.onNodeSecuritySave(password);
        }
        return s;
    }

    private String filterPasswordOnRenderIfNeeded(String password) {
        String s = password;
        if (nodePasswordFilter != null) {
            s = nodePasswordFilter.onNodeSecurityRender(password);
        }
        return s;
    }

    public void checkForOfflineNodes() {
        long offlineNodeDetectionMinutes = parameterService
                .getLong(ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES);
        // Only check for offline nodes if there is a listener and the
        // offline detection period is a positive value. The default value
        // of -1 disables the feature.
        if (offlineServerListeners != null && offlineNodeDetectionMinutes > 0) {

            List<Node> list = findOfflineNodes();
            if (list.size() > 0) {
                fireOffline(list);
            }
        }
    }

    public List<Node> findOfflineNodes() {
        return findOfflineNodes(parameterService
                .getLong(ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES));
    }

    public List<Node> findOfflineNodes(long minutesOffline) {
        List<Node> offlineNodeList = new ArrayList<Node>();
        Node myNode = findIdentity();

        if (myNode != null) {
            long offlineNodeDetectionMillis = minutesOffline * 60 * 1000;

            List<Row> list = sqlTemplate.query(getSql("findOfflineNodesSql"), new Object[] {
                    myNode.getNodeId(), myNode.getNodeId()}, (int[])null);

            for (Row node : list) {
                String nodeId = node.getString("node_id");
                Date time = node.getDateTime("heartbeat_time");
                String offset = node.getString("timezone_offset");
                // Take the timezone of the client node into account when
                // checking the hearbeat time.
                Date clientNodeCurrentTime = null;
                if (offset != null) {
                    clientNodeCurrentTime = AppUtils
                            .getLocalDateForOffset(offset);
                } else {
                    clientNodeCurrentTime = new Date();
                }
                long cutOffTimeMillis = clientNodeCurrentTime.getTime()
                        - offlineNodeDetectionMillis;
                if (time == null
                        || time.getTime() < cutOffTimeMillis) {
                    offlineNodeList.add(findNode(nodeId));
                }
            }
        }

        return offlineNodeList;
    }

    public void setOfflineServerListeners(List<IOfflineServerListener> listeners) {
        this.offlineServerListeners = listeners;
    }

    public void addOfflineServerListener(IOfflineServerListener listener) {
        if (offlineServerListeners == null) {
            offlineServerListeners = new ArrayList<IOfflineServerListener>();
        }
        offlineServerListeners.add(listener);
    }

    public boolean removeOfflineServerListener(IOfflineServerListener listener) {
        if (offlineServerListeners != null) {
            return offlineServerListeners.remove(listener);
        } else {
            return false;
        }
    }

    protected void fireOffline(List<Node> offlineClientNodeList) {
        if (offlineServerListeners != null) {
            for (IOfflineServerListener listener : offlineServerListeners) {
                for (Node node : offlineClientNodeList) {
                    listener.clientNodeOffline(node);
                }
            }
        }
    }

    class NodeRowMapper implements ISqlRowMapper<Node> {
        public Node mapRow(Row rs) {
            Node node = new Node();
            node.setNodeId(rs.getString("node_id"));
            node.setNodeGroupId(rs.getString("node_group_id"));
            node.setExternalId(rs.getString("external_id"));
            node.setSyncEnabled(rs.getBoolean("sync_enabled"));
            node.setSyncUrl(rs.getString("sync_url"));
            node.setSchemaVersion(rs.getString("schema_version"));
            node.setDatabaseType(rs.getString("database_type"));
            node.setDatabaseVersion(rs.getString("database_version"));
            node.setSymmetricVersion(rs.getString("symmetric_version"));
            node.setCreatedAtNodeId(rs.getString("created_at_node_id"));
            node.setBatchToSendCount(rs.getInt("batch_to_send_count"));
            node.setBatchInErrorCount(rs.getInt("batch_in_error_count"));
            node.setDeploymentType(rs.getString("deployment_type"));
            return node;
        }
    }

    class NodeSecurityRowMapper implements ISqlRowMapper<NodeSecurity> {
        public NodeSecurity mapRow(Row rs) {
            NodeSecurity nodeSecurity = new NodeSecurity();
            nodeSecurity.setNodeId(rs.getString("node_id"));
            nodeSecurity.setNodePassword(filterPasswordOnRenderIfNeeded(rs
                    .getString("node_password")));
            nodeSecurity.setRegistrationEnabled(rs.getBoolean("registration_enabled"));
            nodeSecurity.setRegistrationTime(rs.getDateTime("registration_time"));
            nodeSecurity.setInitialLoadEnabled(rs.getBoolean("initial_load_enabled"));
            nodeSecurity.setInitialLoadTime(rs.getDateTime("initial_load_time"));
            nodeSecurity.setCreatedAtNodeId(rs.getString("created_at_node_id"));
            return nodeSecurity;
        }
    }

    class NodeHostRowMapper implements ISqlRowMapper<NodeHost> {
        public NodeHost mapRow(Row rs) {
            NodeHost nodeHost = new NodeHost();
            nodeHost.setNodeId(rs.getString("node_id"));
            nodeHost.setHostName(rs.getString("host_name"));
            nodeHost.setIpAddress(rs.getString("ip_address"));
            nodeHost.setOsUser(rs.getString("os_user"));
            nodeHost.setOsName(rs.getString("os_name"));
            nodeHost.setOsArch(rs.getString("os_arch"));
            nodeHost.setOsVersion(rs.getString("os_version"));
            nodeHost.setAvailableProcessors(rs.getInt("available_processors"));
            nodeHost.setFreeMemoryBytes(rs.getLong("free_memory_bytes"));
            nodeHost.setTotalMemoryBytes(rs.getLong("total_memory_bytes"));
            nodeHost.setMaxMemoryBytes(rs.getLong("max_memory_bytes"));
            nodeHost.setJavaVersion(rs.getString("java_version"));
            nodeHost.setJavaVendor(rs.getString("java_vendor"));
            nodeHost.setSymmetricVersion(rs.getString("symmetric_version"));
            nodeHost.setTimezoneOffset(rs.getString("timezone_offset"));
            nodeHost.setHeartbeatTime(rs.getDateTime("heartbeat_time"));
            nodeHost.setLastRestartTime(rs.getDateTime("last_restart_time"));
            nodeHost.setCreateTime(rs.getDateTime("create_time"));
            return nodeHost;
        }
    }

}
