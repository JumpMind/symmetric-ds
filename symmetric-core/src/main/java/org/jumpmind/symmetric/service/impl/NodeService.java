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
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.config.INodeIdCreator;
import org.jumpmind.symmetric.ext.IOfflineServerListener;
import org.jumpmind.symmetric.model.NetworkedNode;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.security.INodePasswordFilter;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.util.DefaultNodeIdCreator;
import org.jumpmind.util.AppUtils;

/**
 * @see INodeService
 */
public class NodeService extends AbstractService implements INodeService {

	private ISymmetricEngine engine;

    private IExtensionService extensionService;

    private Node cachedNodeIdentity;

    private Map<String, NodeSecurity> securityCache;

    private long securityCacheTime;

    private Map<String, Node> nodeCache = new HashMap<String, Node>();

    private long nodeCacheTime;

    private Map<String, List<Node>> sourceNodesCache = new HashMap<String, List<Node>>();

    private Map<String, List<Node>> targetNodesCache = new HashMap<String, List<Node>>();

    private Map<String, Long> sourceNodeLinkCacheTime = new HashMap<String, Long>(); 
    
    private Map<String, Long> targetNodeLinkCacheTime = new HashMap<String, Long>();

    private INodePasswordFilter nodePasswordFilter;

    private NodeHost nodeHostForCurrentNode = null;

    public NodeService(ISymmetricEngine engine) {
        super(engine.getParameterService(), engine.getSymmetricDialect());
        this.engine = engine;
        extensionService = engine.getExtensionService();
        extensionService.addExtensionPoint(new DefaultNodeIdCreator(parameterService, this, engine.getSecurityService()));
        setSqlMap(new NodeServiceSqlMap(symmetricDialect.getPlatform(), createSqlReplacementTokens()));
    }

    public String findSymmetricVersion() {
        return (String) sqlTemplate.queryForObject(getSql("findSymmetricVersionSql"), String.class);
    }

    public String findIdentityNodeId() {
        Node node = findIdentity();
        return node != null ? node.getNodeId() : null;
    }

    @Override
    public String getExternalId(String nodeId) {
        String externalId = null;
        Node node = findNode(nodeId);
        if (node != null) {
            externalId = node.getExternalId();
        }
        return externalId;
    }

    public Collection<Node> findEnabledNodesFromNodeGroup(String nodeGroupId) {
        return sqlTemplate.query(getSql("selectNodePrefixSql", "findEnabledNodesFromNodeGroupSql"), new NodeRowMapper(),
                new Object[] { nodeGroupId });
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId) {
        return findNodesThatOriginatedFromNodeId(originalNodeId, true);
    }

    public Collection<Node> findNodesWithOpenRegistration() {
        return sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesWithOpenRegistrationSql"), new NodeRowMapper());
    }

    public Set<Node> findNodesThatOriginatedFromNodeId(String originalNodeId, boolean recursive) {
        Set<Node> all = new HashSet<Node>();
        List<Node> list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesCreatedByMeSql"), new NodeRowMapper(), originalNodeId);
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
        List<Node> list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodeSql"), new NodeRowMapper(), id);
        return (Node) getFirstEntry(list);
    } 
    
    public Node findNodeInCacheOnly(String id) {
        return nodeCache.get(id);
    }

    public Node findNode(String id, boolean useCache) {
        if (useCache) {
            long cacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NODE_IN_MS);
            if ((System.currentTimeMillis() - nodeCacheTime) >= cacheTimeoutInMs) {
                nodeCache = findAllNodesAsMap();
                nodeCacheTime = System.currentTimeMillis();
            }
            return nodeCache.get(id);
        } else {
            return findNode(id);
        }
    }

    public void flushNodeCache() {
        nodeCacheTime = 0;
    }

    public Node findNodeByExternalId(String nodeGroupId, String externalId) {
        List<Node> list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodeByExternalIdSql"), new NodeRowMapper(), nodeGroupId,
                externalId);
        return (Node) getFirstEntry(list);
    }

    public void ignoreNodeChannelForExternalId(boolean enabled, String channelId, String nodeGroupId, String externalId) {
        Node node = findNodeByExternalId(nodeGroupId, externalId);
        if (sqlTemplate.update(getSql("nodeChannelControlIgnoreSql"), new Object[] { enabled ? 1 : 0, node.getNodeId(), channelId }) <= 0) {
            sqlTemplate.update(getSql("insertNodeChannelControlSql"), new Object[] { node.getNodeId(), channelId, enabled ? 1 : 0, 0 });
        }
    }

    public List<NodeHost> findNodeHosts(String nodeId) {
        return sqlTemplate.query(getSql("selectNodeHostPrefixSql", "selectNodeHostByNodeIdSql"), new NodeHostRowMapper(), nodeId);
    }

    public void deleteNodeHost(String nodeId) {
        platform.getSqlTemplate().update(getSql("deleteNodeHostSql"), new Object[] { nodeId });
    }

    public void updateNodeHost(NodeHost nodeHost) {

        Object[] params = new Object[] { nodeHost.getIpAddress(), nodeHost.getInstanceId(), nodeHost.getOsUser(), nodeHost.getOsName(), nodeHost.getOsArch(),
                nodeHost.getOsVersion(), nodeHost.getAvailableProcessors(), nodeHost.getFreeMemoryBytes(), nodeHost.getTotalMemoryBytes(),
                nodeHost.getMaxMemoryBytes(), nodeHost.getJavaVersion(), nodeHost.getJavaVendor(), nodeHost.getJdbcVersion(),
                nodeHost.getSymmetricVersion(), nodeHost.getTimezoneOffset(), nodeHost.getHeartbeatTime(), nodeHost.getLastRestartTime(),
                nodeHost.getNodeId(), nodeHost.getHostName()};

        if (sqlTemplate.update(getSql("updateNodeHostSql"), params) <= 0) {
            sqlTemplate.update(getSql("insertNodeHostSql"), params);
        }

    }

    public void updateNodeHostForCurrentNode() {
        if (nodeHostForCurrentNode == null) {
            nodeHostForCurrentNode = new NodeHost(findIdentityNodeId(), engine.getClusterService().getInstanceId());
        }
        nodeHostForCurrentNode.refresh(platform, engine.getClusterService().getInstanceId());
        updateNodeHost(nodeHostForCurrentNode);
    }

    public void deleteNode(String nodeId, boolean syncChange) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            if (!syncChange) {
                symmetricDialect.disableSyncTriggers(transaction, nodeId);
            }
            if (StringUtils.isNotBlank(nodeId)) {
                if (nodeId.equals(findIdentityNodeId())) {
                    transaction.prepareAndExecute(getSql("deleteNodeIdentitySql"));
                }
                transaction.prepareAndExecute(getSql("deleteNodeSecuritySql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteNodeHostSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteNodeSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteNodeChannelCtlSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteIncomingErrorSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteExtractRequestSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteNodeCommunicationSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteTableReloadRequestSql"), new Object[] { nodeId, nodeId });
                transaction.prepareAndExecute(getSql("setOutgoingBatchOkSql"), new Object[] { nodeId });
                transaction.prepareAndExecute(getSql("deleteIncomingBatchSql"), new Object[] { nodeId });
            }
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } finally {
            if (!syncChange) {
                symmetricDialect.enableSyncTriggers(transaction);
            }

            close(transaction);
        }
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
            log.debug(ex.getMessage());
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
                            node.getCreatedAtNodeId(), node.getDeploymentType(), 
                            node.getDeploymentSubType(), node.getConfigVersion(), 
                            node.getNodeId() },
                    new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                            Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                            Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });

            flushNodeGroupCache();
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
                        node.getCreatedAtNodeId(), node.getDeploymentType(), node.getDeploymentSubType(),
                        node.getConfigVersion(), node.getNodeId() },
                new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                        Types.INTEGER, Types.VARCHAR, Types.INTEGER, Types.INTEGER, Types.VARCHAR,
                        Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR }) == 1;

        return updated;
    }

    protected <T> T getFirstEntry(List<T> list) {
        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return null;
    }

    public Node getCachedIdentity() {
        return cachedNodeIdentity;
    }

    public Node findIdentity() {
        return findIdentity(true);
    }

    public Node findIdentity(boolean useCache) {
        return findIdentity(useCache, true);
    }

    public Node findIdentity(boolean useCache, boolean logSqlError) {
        if (cachedNodeIdentity == null || useCache == false) {
            try {
                List<Node> list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodeIdentitySql"), new NodeRowMapper());
                cachedNodeIdentity = (Node) getFirstEntry(list);
            } catch (SqlException ex) {
                if (logSqlError) {
                    // This is at debug level because it gets called pre-registration
                    log.debug("Failed to load the node identity. Returning " + cachedNodeIdentity, ex);
                }
            }
        }
        return cachedNodeIdentity;
    }

    public List<Node> findNodesToPull() {
        return findSourceNodesFor(NodeGroupLinkAction.W);
    }
    
    public List<Node> findNodesWhoPushToMe() {
        return findSourceNodesFor(NodeGroupLinkAction.P);
    }

    public List<Node> findNodesToPushTo() {
        return findTargetNodesFor(NodeGroupLinkAction.P);
    }
    
    public List<Node> findNodesWhoPullFromMe() {
        return findTargetNodesFor(NodeGroupLinkAction.W);
    }

    public List<Node> findSourceNodesFor(NodeGroupLinkAction eventAction) {
        Node node = findIdentity();
        long cacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        if (node != null) {
            List<Node> list = sourceNodesCache.get(eventAction.name());
            if (list == null || (System.currentTimeMillis() - sourceNodeLinkCacheTime.get(eventAction.toString())) >= cacheTimeoutInMs) {
                list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesWhoTargetMeSql"),
                        new NodeRowMapper(), node.getNodeGroupId(), eventAction.name());
                sourceNodesCache.put(eventAction.name(), list);
                sourceNodeLinkCacheTime.put(eventAction.toString(), System.currentTimeMillis());
            }
            return list;
        } else {
            return Collections.emptyList();
        }
    }

    public List<Node> findTargetNodesFor(NodeGroupLinkAction eventAction) {
        Node node = findIdentity();
        long cacheTimeoutInMs = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS);
        if (node != null) {
            List<Node> list = targetNodesCache.get(eventAction.name());
            
            if (list == null || (System.currentTimeMillis() - targetNodeLinkCacheTime.get(eventAction.toString())) >= cacheTimeoutInMs) {
                list = sqlTemplate.query(getSql("selectNodePrefixSql", "findNodesWhoITargetSql"),
                        new NodeRowMapper(), node.getNodeGroupId(), eventAction.name());
                targetNodesCache.put(eventAction.name(), list);
                targetNodeLinkCacheTime.put(eventAction.toString(),System.currentTimeMillis());
            }
            return new ArrayList<Node>(list);
        } else {
            return Collections.emptyList();
        }
    }

    public void flushNodeGroupCache() {
        sourceNodesCache = new HashMap<String, List<Node>>();
        targetNodesCache = new HashMap<String, List<Node>>();
    }

    public List<String> findAllExternalIds() {
        return sqlTemplate.query(getSql("selectExternalIdsSql"), new StringMapper());
    }

    public List<Node> findAllNodes() {
        List<Node> nodeList = sqlTemplate.query(getSql("selectNodePrefixSql"), new NodeRowMapper());
        return nodeList;
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

        nodeLeaf = leaves.get(findIdentityNodeId());
        if (nodeLeaf != null) {
            NetworkedNode root = nodeLeaf.getRoot();
            root.setAllNetworkedNodes(leaves);
            return root;
        } else {
            return null;
        }
    }

    public Node findRootNode() {
        List<Node> nodeList = sqlTemplate.query(getSql("selectNodePrefixSql", "findRootNodeSql"), new NodeRowMapper());
        if (nodeList.size() > 0) {
            return nodeList.get(0);
        }
        return null;
    }
    
    /**
     * Lookup a node_security in the database, which contains private
     * information used to authenticate.
     */
    public NodeSecurity findNodeSecurity(String id) {
        return findNodeSecurity(id, false);
    }

    public NodeSecurity findNodeSecurity(String nodeId, boolean useCache) {
        if (!parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED) && useCache) {
            Map<String, NodeSecurity> nodeSecurities = findAllNodeSecurity(true);
            return nodeSecurities.get(nodeId);
        } else {
            List<NodeSecurity> list = sqlTemplate.query(getSql("findNodeSecuritySql"), new NodeSecurityRowMapper(), new Object[] { nodeId },
                    new int[] { Types.VARCHAR });
            return getFirstEntry(list);
        }
    }

    public NodeSecurity findOrCreateNodeSecurity(String nodeId) {
        try {
            if (nodeId != null) {
                NodeSecurity security = findNodeSecurity(nodeId, false);
                if (security == null) {
                    insertNodeSecurity(nodeId);
                    security = findNodeSecurity(nodeId, true);
                }
                return security;
            } else {
                log.debug("A 'null' node id was passed into findNodeSecurity");
                return null;
            }
        } catch (UniqueKeyException ex) {
            log.error("Could not find a node security row for '{}'", nodeId);
            throw ex;
        }
    }

    public boolean isRegistrationEnabled(String nodeId) {
        NodeSecurity nodeSecurity = findNodeSecurity(nodeId);
        if (nodeSecurity != null) {
            return nodeSecurity.isRegistrationEnabled();
        }
        return false;
    }

    public void insertNodeSecurity(String id) {
        String password = extensionService.getExtensionPoint(INodeIdCreator.class).generatePassword(new Node(id, null, null));
        password = filterPasswordOnSaveIfNeeded(password);
        sqlTemplate.update(getSql("insertNodeSecuritySql"), new Object[] { id, password, null });
        flushNodeAuthorizedCache();
    }

    public void deleteNodeSecurity(String nodeId) {
        sqlTemplate.update(getSql("deleteNodeSecuritySql"), new Object[] { nodeId });
        flushNodeAuthorizedCache();
    }

    public List<NodeSecurity> findNodeSecurityWithLoadEnabled() {
        if (parameterService.is(ParameterConstants.CLUSTER_LOCKING_ENABLED)) {
            return sqlTemplate.query(getSql("findNodeSecurityWithLoadEnabledSql"), new NodeSecurityRowMapper());
        } else {
            List<NodeSecurity> list = new ArrayList<NodeSecurity>();
            for (NodeSecurity nodeSecurity : findAllNodeSecurity(true).values()) {
                if (nodeSecurity.isInitialLoadEnabled() || nodeSecurity.isRevInitialLoadEnabled()) {
                    list.add(nodeSecurity);
                }
            }
            return list;
        }
    }

    public Map<String, NodeSecurity> findAllNodeSecurity(boolean useCache) {
        long maxSecurityCacheTime = parameterService.getLong(ParameterConstants.CACHE_TIMEOUT_NODE_SECURITY_IN_MS);
        Map<String, NodeSecurity> all = securityCache;
        if (all == null || System.currentTimeMillis() - securityCacheTime >= maxSecurityCacheTime || securityCacheTime == 0 || !useCache) {
            all = (Map<String, NodeSecurity>) sqlTemplate.queryForMap(getSql("findAllNodeSecuritySql"), new NodeSecurityRowMapper(),
                    "node_id");
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
        if (nodeSecurity != null && !nodeId.equals(findIdentityNodeId())
                && ((nodeSecurity.getNodePassword() != null && !nodeSecurity.getNodePassword().equals("")
                        && nodeSecurity.getNodePassword().equals(password)) || nodeSecurity.isRegistrationEnabled())) {
            return true;
        }
        return false;
    }

    public void flushNodeAuthorizedCache() {
        securityCacheTime = 0;
    }

    public boolean updateNodeSecurity(NodeSecurity security) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            boolean updated = updateNodeSecurity(transaction, security);
            transaction.commit();
            return updated;
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } finally {
            close(transaction);
        }
    }

    public boolean updateNodeSecurity(ISqlTransaction transaction, NodeSecurity security) {
        security.setNodePassword(filterPasswordOnSaveIfNeeded(security.getNodePassword()));
        int updateCount = transaction.prepareAndExecute(
                getSql("updateNodeSecuritySql"),
                new Object[] { security.getNodePassword(),
                        security.isRegistrationEnabled() ? 1 : 0, security.getRegistrationTime(),
                        security.isInitialLoadEnabled() ? 1 : 0, security.getInitialLoadTime(),
                        security.getCreatedAtNodeId(),
                        security.isRevInitialLoadEnabled() ? 1 : 0,
                        security.getRevInitialLoadTime(),
                        security.getInitialLoadId(),
                        security.getInitialLoadCreateBy(),
                        security.getRevInitialLoadId(),
                        security.getRevInitialLoadCreateBy(),
                        security.getNodeId() }, new int[] {
                        Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP, Types.INTEGER,
                        Types.TIMESTAMP, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP,
                        Types.BIGINT, Types.VARCHAR, Types.BIGINT, Types.VARCHAR,
                        Types.VARCHAR });
        boolean updated = (updateCount == 1);
        flushNodeAuthorizedCache();
        return updated;
    }

    public boolean setInitialLoadEnabled(ISqlTransaction transaction, String nodeId, boolean initialLoadEnabled, boolean syncChange,
            long loadId, String createBy) {
        try {
            if (!syncChange) {
                symmetricDialect.disableSyncTriggers(transaction, nodeId);
            }
            NodeSecurity nodeSecurity = findOrCreateNodeSecurity(nodeId);
            if (nodeSecurity != null) {
                nodeSecurity.setInitialLoadEnabled(initialLoadEnabled);
                nodeSecurity.setInitialLoadId(loadId);
                if (initialLoadEnabled) {
                    nodeSecurity.setInitialLoadTime(null);
                    nodeSecurity.setInitialLoadCreateBy(createBy);
                } else {
                    nodeSecurity.setInitialLoadTime(new Date());
                }
                return updateNodeSecurity(transaction, nodeSecurity);
            }
            return false;
        } finally {
            if (!syncChange) {
                symmetricDialect.enableSyncTriggers(transaction);
            }
        }
    }

    public boolean setInitialLoadEnabled(String nodeId, boolean initialLoadEnabled, boolean syncChange, long loadId, String createBy) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            boolean updated = setInitialLoadEnabled(transaction, nodeId, initialLoadEnabled, syncChange, loadId, createBy);
            transaction.commit();
            return updated;
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } finally {
            close(transaction);
        }
    }

    public boolean setReverseInitialLoadEnabled(ISqlTransaction transaction, String nodeId, boolean initialLoadEnabled, boolean syncChange,
            long loadId, String createBy) {
        try {
            if (!syncChange) {
                symmetricDialect.disableSyncTriggers(transaction, nodeId);
            }

            NodeSecurity nodeSecurity = findOrCreateNodeSecurity(nodeId);
            if (nodeSecurity != null) {
                nodeSecurity.setRevInitialLoadEnabled(initialLoadEnabled);
                nodeSecurity.setRevInitialLoadId(loadId);
                if (initialLoadEnabled) {
                    nodeSecurity.setRevInitialLoadTime(null);
                    nodeSecurity.setRevInitialLoadCreateBy(createBy);
                } else {
                    nodeSecurity.setRevInitialLoadTime(new Date());
                }
                return updateNodeSecurity(transaction, nodeSecurity);
            }
            return false;
        } finally {
            if (!syncChange) {
                symmetricDialect.enableSyncTriggers(transaction);
            }
        }
    }

    public boolean setReverseInitialLoadEnabled(String nodeId, boolean initialLoadEnabled, boolean syncChange, long loadId, String createBy) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            boolean updated = setReverseInitialLoadEnabled(transaction, nodeId, initialLoadEnabled, syncChange, loadId, createBy);
            transaction.commit();
            return updated;
        } catch (Error ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } catch (RuntimeException ex) {
            if (transaction != null) {
                transaction.rollback();
            }
            throw ex;
        } finally {
            close(transaction);
        }
    }

    public boolean isExternalIdRegistered(String nodeGroupId, String externalId) {
        return sqlTemplate.queryForInt(getSql("isNodeRegisteredSql"), new Object[] { nodeGroupId, externalId }) > 0;
    }

    public boolean isDataLoadCompleted() {
        return getNodeStatus() == NodeStatus.DATA_LOAD_COMPLETED;
    }

    public boolean isDataLoadStarted() {
        return getNodeStatus() == NodeStatus.DATA_LOAD_STARTED;
    }

    public boolean isRegistrationServer() {
        return parameterService.isRegistrationServer();
    }

    public NodeStatus getNodeStatus() {
        long ts = System.currentTimeMillis();
        try {
            NodeSecurity nodeSecurity = findNodeSecurity(findIdentityNodeId(), true);
            if (nodeSecurity != null) {
                if (nodeSecurity.isInitialLoadEnabled()) {
                    return NodeStatus.DATA_LOAD_STARTED;
                } else if (nodeSecurity.getInitialLoadTime() != null) {
                    return NodeStatus.DATA_LOAD_COMPLETED;
                }
            }
            return NodeStatus.DATA_LOAD_NOT_STARTED;
        } catch (SqlException ex) {
            log.error("Could not query table after {} ms.  The status is unknown.", (System.currentTimeMillis() - ts), ex);
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
        long offlineNodeDetectionMinutes = parameterService.getLong(ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES);
        List<IOfflineServerListener> offlineServerListeners = extensionService.getExtensionPointList(IOfflineServerListener.class);
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
        return findOfflineNodes(parameterService.getLong(ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES));
    }

    public List<Node> findOfflineNodes(long minutesOffline) {
        List<Node> offlineNodeList = new ArrayList<Node>();
        Node myNode = findIdentity();
        long restartDelayMinutes = parameterService.getLong(ParameterConstants.OFFLINE_NODE_DETECTION_RESTART_MINUTES);

        Date lastRestartTime = engine.getLastRestartTime() != null ? engine.getLastRestartTime() : new Date();
        if (myNode != null && System.currentTimeMillis() - lastRestartTime.getTime() > restartDelayMinutes * 60000) {
            long offlineNodeDetectionMillis = minutesOffline * 60 * 1000;

            List<Row> list = sqlTemplate.query(getSql("findNodeHeartbeatsSql"));
            for (Row node : list) {
                String nodeId = node.getString("node_id");
                Date time = node.getDateTime("heartbeat_time");
                String offset = node.getString("timezone_offset");
                // Take the timezone of the client node into account when
                // checking the hearbeat time.
                Date clientNodeCurrentTime = null;
                if (offset != null) {
                    clientNodeCurrentTime = AppUtils.getLocalDateForOffset(offset);
                } else {
                    clientNodeCurrentTime = new Date();
                }
                long cutOffTimeMillis = clientNodeCurrentTime.getTime() - offlineNodeDetectionMillis;
                if (time == null || time.getTime() < cutOffTimeMillis) {
                    offlineNodeList.add(findNode(nodeId));
                }
            }
        }

        return offlineNodeList;
    }

    public Map<String, Date> findLastHeartbeats() {
        Map<String, Date> dates = new HashMap<String, Date>();
        Node myNode = findIdentity();
        if (myNode != null) {
            List<Row> list = sqlTemplate.query(getSql("findNodeHeartbeatsSql"));
            for (Row node : list) {
                String nodeId = node.getString("node_id");
                Date time = node.getDateTime("heartbeat_time");
                dates.put(nodeId, time);
            }
        }
        return dates;
    }

    public List<String> findOfflineNodeIds(long minutesOffline) {
        List<String> offlineNodeList = new ArrayList<String>();
        Node myNode = findIdentity();

        if (myNode != null) {
            long offlineNodeDetectionMillis = minutesOffline * 60 * 1000;

            List<Row> list = sqlTemplate.query(getSql("findNodeHeartbeatsSql"));
            for (Row node : list) {
                String nodeId = node.getString("node_id");
                Date time = node.getDateTime("heartbeat_time");
                String offset = node.getString("timezone_offset");
                // Take the timezone of the client node into account when
                // checking the hearbeat time.
                Date clientNodeCurrentTime = null;
                if (offset != null) {
                    clientNodeCurrentTime = AppUtils.getLocalDateForOffset(offset);
                } else {
                    clientNodeCurrentTime = new Date();
                }
                long cutOffTimeMillis = clientNodeCurrentTime.getTime() - offlineNodeDetectionMillis;
                if (time == null || time.getTime() < cutOffTimeMillis) {
                    offlineNodeList.add(nodeId);
                }
            }
        }
        return offlineNodeList;
    }

    protected void fireOffline(List<Node> offlineClientNodeList) {
        for (IOfflineServerListener listener : extensionService.getExtensionPointList(IOfflineServerListener.class)) {
            for (Node node : offlineClientNodeList) {
                listener.clientNodeOffline(node);
            }
        }
    }
        
    static class NodeRowMapper implements ISqlRowMapper<Node> {
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
            node.setDeploymentSubType(rs.getString("deployment_sub_type"));
            node.setConfigVersion(rs.getString("config_version"));
            return node;
        }
    }

    class NodeSecurityRowMapper implements ISqlRowMapper<NodeSecurity> {
        public NodeSecurity mapRow(Row rs) {
            NodeSecurity nodeSecurity = new NodeSecurity();
            nodeSecurity.setNodeId(rs.getString("node_id"));
            nodeSecurity.setNodePassword(filterPasswordOnRenderIfNeeded(rs.getString("node_password")));
            nodeSecurity.setRegistrationEnabled(rs.getBoolean("registration_enabled"));
            nodeSecurity.setRegistrationTime(rs.getDateTime("registration_time"));
            nodeSecurity.setInitialLoadEnabled(rs.getBoolean("initial_load_enabled"));
            nodeSecurity.setInitialLoadTime(rs.getDateTime("initial_load_time"));
            nodeSecurity.setCreatedAtNodeId(rs.getString("created_at_node_id"));
            nodeSecurity.setRevInitialLoadEnabled(rs.getBoolean("rev_initial_load_enabled"));
            nodeSecurity.setRevInitialLoadTime(rs.getDateTime("rev_initial_load_time"));
            nodeSecurity.setInitialLoadId(rs.getLong("initial_load_id"));
            nodeSecurity.setInitialLoadCreateBy(rs.getString("initial_load_create_by"));
            nodeSecurity.setRevInitialLoadId(rs.getLong("rev_initial_load_id"));
            nodeSecurity.setRevInitialLoadCreateBy(rs.getString("rev_initial_load_create_by"));
            return nodeSecurity;
        }
    }

    static class NodeHostRowMapper implements ISqlRowMapper<NodeHost> {
        public NodeHost mapRow(Row rs) {
            NodeHost nodeHost = new NodeHost();
            nodeHost.setNodeId(rs.getString("node_id"));
            nodeHost.setHostName(rs.getString("host_name"));
            nodeHost.setInstanceId(rs.getString("instance_id"));
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
            nodeHost.setJdbcVersion(rs.getString("jdbc_version"));
            nodeHost.setSymmetricVersion(rs.getString("symmetric_version"));
            nodeHost.setTimezoneOffset(rs.getString("timezone_offset"));
            nodeHost.setHeartbeatTime(rs.getDateTime("heartbeat_time"));
            nodeHost.setLastRestartTime(rs.getDateTime("last_restart_time"));
            nodeHost.setCreateTime(rs.getDateTime("create_time"));
            return nodeHost;
        }
    }
    
    public AuthenticationStatus getAuthenticationStatus(String nodeId, String securityToken) {
        AuthenticationStatus retVal = AuthenticationStatus.ACCEPTED;
        Node node = findNode(nodeId, true);
        if (node == null) {
            retVal = AuthenticationStatus.REGISTRATION_REQUIRED;
        } else if (!syncEnabled(node)) {
            if(registrationOpen(node)){
                retVal = AuthenticationStatus.REGISTRATION_REQUIRED;
            }else{
                retVal = AuthenticationStatus.SYNC_DISABLED;
            }
        } else if (!isNodeAuthorized(nodeId, securityToken)) {
            retVal = AuthenticationStatus.FORBIDDEN;
        }
        return retVal;
    }

    protected boolean syncEnabled(Node node) {
        boolean syncEnabled = false;
        if (node != null) {
            syncEnabled = node.isSyncEnabled();
        }
        return syncEnabled;
    }

    protected boolean registrationOpen(Node node){
        NodeSecurity security = findNodeSecurity(node.getNodeId());
        if(security != null){
            return security.isRegistrationEnabled();
        }
        return false;
    } 

}
