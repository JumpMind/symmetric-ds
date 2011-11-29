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

import java.sql.ResultSet;
import java.sql.SQLException;
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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.INodeIdGenerator;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.dao.CannotAcquireLockException;
import org.springframework.dao.DataAccessException;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.ResultSetExtractor;
import org.springframework.jdbc.core.RowMapper;

/**
 * @see INodeService
 */
public class NodeService extends AbstractService implements INodeService {
    
    private Node cachedNodeIdentity;

    private Map<String, NodeSecurity> securityCache;

    private long securityCacheTime;

    private INodeIdGenerator nodeIdGenerator;

    private INodePasswordFilter nodePasswordFilter;
    
    private NodeHost nodeHostForCurrentNode = null;
    
    private long offlineNodeDetectionMinutes;
        
    private List<IOfflineServerListener> offlineServerListeners;

    public String findSymmetricVersion() {
        try {
            return (String) jdbcTemplate.queryForObject(getSql("findSymmetricVersionSql"), String.class);
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    public String findIdentityNodeId() {
        Node node = findIdentity();
        return node != null ? node.getNodeId() : null;
    }
    
    public Collection<Node> findEnabledNodesFromNodeGroup(String nodeGroupId) {
        return jdbcTemplate.query(getSql("selectNodePrefixSql","findEnabledNodesFromNodeGroupSql"), new Object[] { nodeGroupId }, new NodeRowMapper());
    }
    
    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId) {
        return findNodesThatOriginatedFromNodeId(originalNodeId, true);
    }
    
    public Collection<Node> findNodesWithOpenRegistration() {
        return jdbcTemplate.query(getSql("selectNodePrefixSql","findNodesWithOpenRegistrationSql"), new NodeRowMapper());
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId, boolean recursive) {
        Set<Node> all = new HashSet<Node>();
        List<Node> list = jdbcTemplate.query(getSql("selectNodePrefixSql","findNodesCreatedByMeSql"), new Object[] { originalNodeId }, new NodeRowMapper());
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
        List<Node> list = jdbcTemplate.query(getSql("selectNodePrefixSql","findNodeSql"),
                new Object[] { id }, new NodeRowMapper());
        return (Node) getFirstEntry(list);
    }

    public Node findNodeByExternalId(String nodeGroupId, String externalId) {
        List<Node> list = jdbcTemplate.query(getSql("selectNodePrefixSql","findNodeByExternalIdSql"),
                new Object[] { nodeGroupId, externalId }, new NodeRowMapper());
        return (Node) getFirstEntry(list);
    }

    public void ignoreNodeChannelForExternalId(boolean enabled, String channelId, String nodeGroupId, String externalId) {
        Node node = findNodeByExternalId(nodeGroupId, externalId);
        if (jdbcTemplate.update(getSql("nodeChannelControlIgnoreSql"), new Object[] { enabled ? 1 : 0,
                node.getNodeId(), channelId }) == 0) {
            jdbcTemplate.update(getSql("insertNodeChannelControlSql"), new Object[] { node.getNodeId(), channelId,
                    enabled ? 1 : 0, 0 });
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
        return jdbcTemplate.query(getSql("selectNodeHostPrefixSql", "selectNodeHostByNodeIdSql"), new NodeHostRowMapper(), nodeId);
    }
    
    public void updateNodeHostForCurrentNode() {
        if (nodeHostForCurrentNode == null) {
            nodeHostForCurrentNode = new NodeHost(findIdentityNodeId());
        }
        nodeHostForCurrentNode.refresh();        
        Object[] params = new Object[] {
                nodeHostForCurrentNode.getIpAddress(),
                nodeHostForCurrentNode.getOsUser(),
                nodeHostForCurrentNode.getOsName(),
                nodeHostForCurrentNode.getOsArch(),
                nodeHostForCurrentNode.getOsVersion(),
                nodeHostForCurrentNode.getAvailableProcessors(),
                nodeHostForCurrentNode.getFreeMemoryBytes(),
                nodeHostForCurrentNode.getTotalMemoryBytes(),
                nodeHostForCurrentNode.getMaxMemoryBytes(),
                nodeHostForCurrentNode.getJavaVersion(),
                nodeHostForCurrentNode.getJavaVendor(),
                nodeHostForCurrentNode.getSymmetricVersion(),
                nodeHostForCurrentNode.getTimezoneOffset(),
                nodeHostForCurrentNode.getHeartbeatTime(),
                nodeHostForCurrentNode.getLastRestartTime(),
                nodeHostForCurrentNode.getNodeId(),
                nodeHostForCurrentNode.getHostName()
            };
        if (jdbcTemplate.update(getSql("updateNodeHostSql"), params) == 0) {
            jdbcTemplate.update(getSql("insertNodeHostSql"), params);
        }
    }

    public NodeSecurity findNodeSecurity(String nodeId, boolean createIfNotFound) {
        try {
            if (nodeId != null) {
                List<NodeSecurity> list = jdbcTemplate.query(getSql("findNodeSecuritySql"), new Object[] { nodeId },
                        new int[] { Types.VARCHAR }, new NodeSecurityRowMapper());
                NodeSecurity security = (NodeSecurity) getFirstEntry(list);
                if (security == null && createIfNotFound) {
                    insertNodeSecurity(nodeId);
                    security = findNodeSecurity(nodeId, false);
                } 
                return security;
            } else {
                log.debug("FindNodeSecurityNodeNull");
                return null;
            }
        } catch (DataIntegrityViolationException ex) {
            log.error("NodeSecurityMissing", nodeId);
            throw ex;
        }
    }
    
    public void deleteNodeSecurity(String nodeId) {
        jdbcTemplate.update(getSql("deleteNodeSecuritySql"), new Object[] { nodeId });
    }
    
    public void deleteNode(String nodeId) {
        jdbcTemplate.update(getSql("deleteNodeSql"), new Object[] { nodeId });
    }

    public void insertNodeSecurity(String id) {
        flushNodeAuthorizedCache();
        String password = nodeIdGenerator.generatePassword(this, new Node(id, null, null));
        password = filterPasswordOnSaveIfNeeded(password);
        jdbcTemplate.update(getSql("insertNodeSecuritySql"), new Object[] { id, password, findIdentity().getNodeId() });
    }
    
    public void insertNodeIdentity(String nodeId) {
        jdbcTemplate.update(getSql("insertNodeIdentitySql"), nodeId);
    }
    
    public void deleteIdentity() {
        jdbcTemplate.execute(getSql("deleteNodeIdentitySql"));
        cachedNodeIdentity = null;
    }
    
    public void insertNode(String nodeId, String nodeGroupdId, String externalId, String createdAtNodeId) {
        jdbcTemplate.update(getSql("insertNodeSql"), new Object[] { nodeId, nodeGroupdId,
            externalId, createdAtNodeId, AppUtils.getTimezoneOffset() });
    }
    
    public void insertNodeGroup(String groupId, String description) {
        if (jdbcTemplate.queryForInt(getSql("doesNodeGroupExistSql"), groupId) == 0) {
            jdbcTemplate.update(getSql("insertNodeGroupSql"), description, groupId);
        }
    }

    public boolean updateNode(Node node) {
        boolean updated = jdbcTemplate.update(getSql("updateNodeSql"), new Object[] { node.getNodeGroupId(),
                node.getExternalId(), node.getDatabaseType(), node.getDatabaseVersion(), node.getSchemaVersion(),
                node.getSymmetricVersion(), node.getSyncUrl(), node.getHeartbeatTime(), node.isSyncEnabled() ? 1 : 0,
                node.getTimezoneOffset(), node.getBatchToSendCount(), node.getBatchInErrorCount(), node.getCreatedAtNodeId(), node.getDeploymentType(), node.getNodeId() }, new int[] { Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.TIMESTAMP, Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }) == 1;
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
        Map<String,NodeSecurity> all = securityCache;
        if (all == null || System.currentTimeMillis() - securityCacheTime >= maxSecurityCacheTime
                || securityCacheTime == 0 || !useCache) {
            all = (Map<String, NodeSecurity>) jdbcTemplate.query(
                    getSql("findAllNodeSecuritySql"), new NodeSecurityResultSetExtractor());
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
        Map<String,NodeSecurity> nodeSecurities = findAllNodeSecurity(true);
        NodeSecurity nodeSecurity = nodeSecurities.get(nodeId);
        if (nodeSecurity != null
                && ((nodeSecurity.getNodePassword() != null && !nodeSecurity.getNodePassword().equals("") && nodeSecurity
                        .getNodePassword().equals(password)) || nodeSecurity.isRegistrationEnabled())) {
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
            List<Node> list = jdbcTemplate.query(getSql("selectNodePrefixSql","findNodeIdentitySql"),
                    new NodeRowMapper());
            cachedNodeIdentity = (Node) getFirstEntry(list);
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
            return jdbcTemplate.query(getSql("selectNodePrefixSql","findNodesWhoTargetMeSql"), new Object[] {
                    node.getNodeGroupId(), eventAction.name() }, new NodeRowMapper());
        } else {
            return Collections.emptyList();
        }
    }

    public List<Node> findTargetNodesFor(NodeGroupLinkAction eventAction) {
        Node node = findIdentity();
        if (node != null) {
            return jdbcTemplate.query(getSql("selectNodePrefixSql","findNodesWhoITargetSql"), new Object[] {
                    node.getNodeGroupId(), eventAction.name() }, new NodeRowMapper());
        } else {
            return Collections.emptyList();
        }
    }
    
    public List<String> findAllExternalIds() {     
        return jdbcTemplate.queryForList(getSql("selectExternalIdsSql"), String.class);
    }
    
    public List<Node> findAllNodes() {
        return jdbcTemplate.query(getSql("selectNodePrefixSql"), new NodeRowMapper());        
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
        return nodeLeaf.getRoot();
    }

    public boolean updateNodeSecurity(NodeSecurity security) {
        flushNodeAuthorizedCache();
        security.setNodePassword(filterPasswordOnSaveIfNeeded(security.getNodePassword()));
        return jdbcTemplate.update(getSql("updateNodeSecuritySql"), new Object[] { security.getNodePassword(),
                security.isRegistrationEnabled() ? 1 : 0, security.getRegistrationTime(),
                security.isInitialLoadEnabled() ? 1 : 0, security.getInitialLoadTime(), security.getCreatedAtNodeId(),
                security.getNodeId() }, new int[] { Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP, Types.INTEGER,
                Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR }) == 1;
    }

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled) {
        NodeSecurity nodeSecurity = findNodeSecurity(nodeId, true);
        if (nodeSecurity != null) {
            nodeSecurity.setInitialLoadEnabled(initialLoadEnabled);
            if (initialLoadEnabled) {
                nodeSecurity.setInitialLoadTime(null);
            } else {
                nodeSecurity.setInitialLoadTime(new Date());
            }
            return updateNodeSecurity(nodeSecurity);
        }
        return false;
    }

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId) {
        return jdbcTemplate.queryForInt(getSql("isNodeRegisteredSql"), new Object[] { nodeGroupId, externalId }) > 0;
    }

    public INodeIdGenerator getNodeIdGenerator() {
        return nodeIdGenerator;
    }

    public void setNodeIdGenerator(INodeIdGenerator nodeIdGenerator) {
        this.nodeIdGenerator = nodeIdGenerator;
    }

    public boolean isDataLoadCompleted() {
        return getNodeStatus() == NodeStatus.DATA_LOAD_COMPLETED;
    }

    public boolean isDataLoadStarted() {
        return getNodeStatus() == NodeStatus.DATA_LOAD_STARTED;
    }

    public boolean isRegistrationServer() {
        return StringUtils.isBlank(parameterService.getRegistrationUrl()) || parameterService.getRegistrationUrl().equals(parameterService.getSyncUrl());
    }

    public NodeStatus getNodeStatus() {
        long ts = System.currentTimeMillis();
        try {
            class DataLoadStatus {
                int initialLoadEnabled;
                Date initialLoadTime;
            }

            List<DataLoadStatus> results = jdbcTemplate.query(getSql("getDataLoadStatusSql"), new RowMapper<DataLoadStatus>() {
                public DataLoadStatus mapRow(java.sql.ResultSet rs, int arg1) throws java.sql.SQLException {
                    DataLoadStatus status = new DataLoadStatus();
                    status.initialLoadEnabled = rs.getInt(1);
                    status.initialLoadTime = rs.getTimestamp(2);
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
        } catch (CannotAcquireLockException ex) {
            log.error("LockAcquiringFailed", (System.currentTimeMillis() - ts));
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
        // Only check for offline nodes if there is a listener and the
        // offline detection period is a positive value.  The default value
        // of -1 disables the feature.
        if (offlineServerListeners != null && getOfflineNodeDetectionMinutes() > 0) {
            
            List<Node> list = findOfflineNodes();
            if (list.size() > 0) {
                fireOffline(list);
            }
        }
    }
    
    public List<Node> findOfflineNodes() {
        return findOfflineNodes(getOfflineNodeDetectionMinutes());
    }
    
    public List<Node> findOfflineNodes(long minutesOffline) {
        List<Node> offlineNodeList = new ArrayList<Node>();
        Node myNode = findIdentity();
        
        if (myNode != null) {
            long offlineNodeDetectionMillis =  minutesOffline*60*1000;
            
            List<Node> list = jdbcTemplate.query(getSql("selectNodePrefixSql","findOfflineNodesSql"), 
                    new Object[] { myNode.getNodeId(),  myNode.getNodeId()}, 
                    new NodeRowMapper());
            
            for (Node node: list) {
                // Take the timezone of the client node into account when checking the hearbeat time.
                Date clientNodeCurrentTime = null;
                if (node.getTimezoneOffset() != null) {
                    clientNodeCurrentTime = AppUtils.getLocalDateForOffset(node.getTimezoneOffset());
                } else {
                    clientNodeCurrentTime = new Date();
                }
                long cutOffTimeMillis = clientNodeCurrentTime.getTime() - offlineNodeDetectionMillis;
                if (node.getHeartbeatTime() == null  || node.getHeartbeatTime().getTime() < cutOffTimeMillis) {
                    offlineNodeList.add(node);
                }
            }
        }
        
        return offlineNodeList;
    }
    
    public long getOfflineNodeDetectionMinutes() {
        return offlineNodeDetectionMinutes;
    }
    
    public void setOfflineNodeDetectionMinutes(long offlineNodeDetectionMinutes) {
        this.offlineNodeDetectionMinutes = offlineNodeDetectionMinutes;
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
    

    class NodeRowMapper implements RowMapper<Node> {
        public Node mapRow(ResultSet rs, int num) throws SQLException {
            Node node = new Node();
            node.setNodeId(rs.getString(1));
            node.setNodeGroupId(rs.getString(2));
            node.setExternalId(rs.getString(3));
            node.setSyncEnabled(rs.getBoolean(4));
            node.setSyncUrl(rs.getString(5));
            node.setSchemaVersion(rs.getString(6));
            node.setDatabaseType(rs.getString(7));
            node.setDatabaseVersion(rs.getString(8));
            node.setSymmetricVersion(rs.getString(9));
            node.setCreatedAtNodeId(rs.getString(10));
            node.setHeartbeatTime(rs.getTimestamp(11));
            node.setTimezoneOffset(rs.getString(12));
            node.setBatchToSendCount(rs.getInt(13));
            node.setBatchInErrorCount(rs.getInt(14));
            node.setDeploymentType(rs.getString(15));
            return node;
        }
    }
    
    class NodeSecurityRowMapper implements RowMapper<NodeSecurity> {
        public NodeSecurity mapRow(ResultSet rs, int num) throws SQLException {
            NodeSecurity nodeSecurity = new NodeSecurity();
            nodeSecurity.setNodeId(rs.getString(1));
            nodeSecurity.setNodePassword(filterPasswordOnRenderIfNeeded(rs.getString(2)));
            nodeSecurity.setRegistrationEnabled(rs.getBoolean(3));
            nodeSecurity.setRegistrationTime(rs.getTimestamp(4));
            nodeSecurity.setInitialLoadEnabled(rs.getBoolean(5));
            nodeSecurity.setInitialLoadTime(rs.getTimestamp(6));
            nodeSecurity.setCreatedAtNodeId(rs.getString(7));
            return nodeSecurity;
        }
    }

    class NodeSecurityResultSetExtractor implements ResultSetExtractor<Map<String, NodeSecurity>> {
        public Map<String, NodeSecurity> extractData(ResultSet rs) throws SQLException, DataAccessException {
            Map<String, NodeSecurity> result = new HashMap<String, NodeSecurity>();
            NodeSecurityRowMapper mapper = new NodeSecurityRowMapper();

            while (rs.next()) {
                NodeSecurity nodeSecurity = (NodeSecurity) mapper.mapRow(rs, 0);
                result.put(nodeSecurity.getNodeId(), nodeSecurity);
            }
            return result;
        }
    }
    
    class NodeHostRowMapper implements RowMapper<NodeHost> {
        public NodeHost mapRow(ResultSet rs, int rowNum) throws SQLException {
            NodeHost nodeHost = new NodeHost();
            nodeHost.setNodeId(rs.getString(1));
            nodeHost.setHostName(rs.getString(2));
            nodeHost.setIpAddress(rs.getString(3));
            nodeHost.setOsUser(rs.getString(4));
            nodeHost.setOsName(rs.getString(5));
            nodeHost.setOsArch(rs.getString(6));
            nodeHost.setOsVersion(rs.getString(7));
            nodeHost.setAvailableProcessors(rs.getInt(8));
            nodeHost.setFreeMemoryBytes(rs.getLong(9));
            nodeHost.setTotalMemoryBytes(rs.getLong(10));
            nodeHost.setMaxMemoryBytes(rs.getLong(11));
            nodeHost.setJavaVersion(rs.getString(12));
            nodeHost.setJavaVendor(rs.getString(13));
            nodeHost.setSymmetricVersion(rs.getString(14));
            nodeHost.setTimezoneOffset(rs.getString(15));
            nodeHost.setHeartbeatTime(rs.getTimestamp(16));
            nodeHost.setLastRestartTime(rs.getTimestamp(17));
            nodeHost.setCreateTime(rs.getTimestamp(18));
            return nodeHost;
        }
    }        
    
}