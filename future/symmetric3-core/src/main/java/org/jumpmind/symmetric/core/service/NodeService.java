package org.jumpmind.symmetric.core.service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.SymmetricTables;
import org.jumpmind.symmetric.core.db.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.db.Query;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.db.mapper.NodeMapper;
import org.jumpmind.symmetric.core.db.mapper.NodeSecurityMapper;
import org.jumpmind.symmetric.core.ext.INodePasswordFilter;
import org.jumpmind.symmetric.core.model.Node;
import org.jumpmind.symmetric.core.model.NodeGroup;
import org.jumpmind.symmetric.core.model.NodeSecurity;

public class NodeService extends AbstractParameterizedService {

    private final static NodeMapper NODE_MAPPER = new NodeMapper();

    private final static NodeSecurityMapper NODE_SECURITY_MAPPER = new NodeSecurityMapper();

    private Node cachedNodeIdentity;

    private INodePasswordFilter nodePasswordFilter;

    public NodeService(IEnvironment environment, ParameterService parameterService) {
        super(environment, parameterService);
    }

    public Node findIdentity() {
        return findIdentity(true);
    }

    public void saveNode(Node node) {
        dbDialect.getSqlTemplate().save(getTable(SymmetricTables.NODE), getParams(node));
    }

    public void saveNodeGroup(NodeGroup nodeGroup) {
        dbDialect.getSqlTemplate().save(getTable(SymmetricTables.NODE_GROUP), getParams(nodeGroup));
    }

    public void deleteNode(Node node) {
        dbDialect.getSqlTemplate().delete(getTable(SymmetricTables.NODE), getParams(node));
    }

    public void saveNodeSecurity(NodeSecurity node) {
        dbDialect.getSqlTemplate().save(getTable(SymmetricTables.NODE_SECURITY), getParams(node));
    }

    public void deleteNodeSecurity(NodeSecurity node) {
        dbDialect.getSqlTemplate().delete(getTable(SymmetricTables.NODE_SECURITY), getParams(node));
    }

    private String filterPasswordOnRenderIfNeeded(String password) {
        String s = password;
        if (nodePasswordFilter != null) {
            s = nodePasswordFilter.onNodeSecurityRender(password);
        }
        return s;
    }

    /**
     * Lookup a node in the database, which contains information for syncing
     * with it.
     */
    public Node findNode(String id) {
        List<Node> list = dbDialect.getSqlTemplate().query(
                new Query(dbDialect.getDbDialectInfo().getIdentifierQuoteString(), 1,
                        getTable(SymmetricTables.NODE)).where("NODE_ID", "=", id), NODE_MAPPER);
        return (Node) getFirstEntry(list);
    }

    /**
     * Lookup a node_security in the database, which contains private
     * information used to authenticate.
     */
    public NodeSecurity findNodeSecurity(String id) {
        return findNodeSecurity(id, false);
    }

    public NodeSecurity findNodeSecurity(String nodeId, boolean createIfNotFound) {
        try {
            if (nodeId != null) {
                List<NodeSecurity> list = dbDialect.getSqlTemplate().query(
                        new Query(dbDialect.getDbDialectInfo().getIdentifierQuoteString(), 1,
                                getTables(SymmetricTables.NODE_SECURITY)).where("NODE_ID", "=",
                                nodeId), NODE_SECURITY_MAPPER);

                NodeSecurity security = (NodeSecurity) getFirstEntry(list);
                if (security == null && createIfNotFound) {
                    insertNodeSecurity(nodeId);
                    security = findNodeSecurity(nodeId, false);
                }
                if (security != null) {
                    security.setNodePassword(filterPasswordOnRenderIfNeeded(security
                            .getNodePassword()));
                }
                return security;
            } else {
                log.debug("Did not find a node security row for %s", nodeId);
                return null;
            }
        } catch (DataIntegrityViolationException ex) {
            log.error(
                    "Could not find a node security row for %s.  A node needs a matching security row in both the local and remote nodes if it is going to authenticate to push data.",
                    nodeId);
            throw ex;
        }
    }

    public void insertNodeSecurity(String nodeId) {
        dbDialect.getSqlTemplate().insert(getTable(SymmetricTables.NODE_SECURITY),
                new Row("NODE_ID", nodeId));
    }

    public void deleteNodeIdentity(String nodeId) {
        dbDialect.getSqlTemplate().delete(tables.getSymmetricTable(SymmetricTables.NODE_IDENTITY),
                new Row("NODE_ID", nodeId));
    }

    public String findIdentityNodeId() {
        Node node = findIdentity();
        return node != null ? node.getNodeId() : null;
    }

    public Node findIdentity(boolean useCache) {
        if (cachedNodeIdentity == null || useCache == false) {
            List<Node> list = dbDialect.getSqlTemplate().query(
                    new Query(dbDialect.getDbDialectInfo().getIdentifierQuoteString(), 0,
                            tables.getSymmetricTables(SymmetricTables.NODE,
                                    SymmetricTables.NODE_IDENTITY)), NODE_MAPPER);
            cachedNodeIdentity = getFirstEntry(list);
        }
        return cachedNodeIdentity;
    }

    public void insertNodeIdentity(String nodeId) {
        dbDialect.getSqlTemplate().save(getTable(SymmetricTables.NODE_IDENTITY),
                new Row("NODE_ID", nodeId));
    }

    protected Map<String, Object> getParams(NodeGroup nodeGroup) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("NODE_GROUP_ID", nodeGroup.getNodeGroupId());
        params.put("DESCRIPTION", nodeGroup.getDescription());
        return params;
    }

    protected Map<String, Object> getParams(NodeSecurity nodeSecurity) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("NODE_ID", nodeSecurity.getNodeId());
        params.put("NODE_PASSWORD", nodeSecurity.getNodePassword());
        params.put("REGISTRATION_ENABLED", nodeSecurity.isRegistrationEnabled());
        params.put("REGISTRATION_TIME", nodeSecurity.getRegistrationTime());
        params.put("INITIAL_LOAD_ENABLED", nodeSecurity.isInitialLoadEnabled());
        params.put("INITIAL_LOAD_TIME", nodeSecurity.getInitialLoadTime());
        params.put("CREATED_AT_NODE_ID", nodeSecurity.getCreatedAtNodeId());
        return params;
    }

    protected Map<String, Object> getParams(Node node) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("NODE_ID", node.getNodeId());
        params.put("NODE_GROUP_ID", node.getNodeGroupId());
        params.put("EXTERNAL_ID", node.getExternalId());
        params.put("SYNC_ENABLED", node.isSyncEnabled());
        params.put("SYNC_URL", node.getSyncUrl());
        params.put("SCHEMA_VERSION", node.getSchemaVersion());
        params.put("DATABASE_TYPE", node.getDatabaseType());
        params.put("DATABASE_VERSION", node.getDatabaseVersion());
        params.put("SYMMETRIC_VERSION", node.getSymmetricVersion());
        params.put("CREATED_AT_NODE_ID", node.getCreatedAtNodeId());
        params.put("HEARTBEAT_TIME", node.getHeartbeatTime());
        params.put("TIMEZONE_OFFSET", node.getTimezoneOffset());
        params.put("BATCH_TO_SEND_COUNT", node.getBatchToSendCount());
        params.put("BATCH_IN_ERROR_COUNT", node.getBatchInErrorCount());
        params.put("DEPLOYMENT_TYPE", node.getDeploymentType());
        return params;
    }

    public void setNodePasswordFilter(INodePasswordFilter nodePasswordFilter) {
        this.nodePasswordFilter = nodePasswordFilter;
    }

    public INodePasswordFilter getNodePasswordFilter() {
        return nodePasswordFilter;
    }

}
