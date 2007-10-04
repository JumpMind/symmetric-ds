package org.jumpmind.symmetric.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.jdbc.core.RowMapper;

public class NodeService extends AbstractService implements INodeService {

    protected static final Log logger = LogFactory.getLog(NodeService.class);

    protected String findNodeSql;

    protected String findNodeSecuritySql;

    protected String findNodeIdentitySql;

    protected String findNodesWhoTargetMeSql;

    protected String findNodesWhoITargetSql;

    protected String updateNodeSql;

    /**
     * Lookup a client in the database, which contains information for synching
     * with it.
     */
    @SuppressWarnings("unchecked")
    public Node findNode(String id) {
        List<Node> list = jdbcTemplate.query(findNodeSql, new Object[] { id },
                new NodeRowMapper());
        return (Node) getFirstEntry(list);
    }

    /**
     * Lookup a client_security in the database, which contains private
     * information used to authenticate.
     */
    @SuppressWarnings("unchecked")
    public NodeSecurity findNodeSecurity(String id) {
        List<NodeSecurity> list = jdbcTemplate.query(findNodeSecuritySql,
                new Object[] { id }, new NodeSecurityRowMapper());
        return (NodeSecurity) getFirstEntry(list);
    }

    public boolean updateNode(Node node) {
        boolean updated = jdbcTemplate.update(updateNodeSql, new Object[] {
                node.getNodeGroupId(), node.getExternalId(),
                node.getDatabaseType(), node.getDatabaseVersion(),
                node.getSchemaVersion(), node.getSymmetricVersion(),
                node.getSyncURL(), node.getHeartbeatTime(), node.isSyncEnabled(), node.getNodeId() }) == 1;        
        return updated;
    }

    protected <T> T getFirstEntry(List<T> list) {
        if (list != null && list.size() > 0) {
            return list.get(0);
        }
        return null;
    }

    /**
     * Check that the given client and password match in the client_security
     * table. A client must authenticate before it's allowed to sync data.
     */
    public boolean isNodeAuthorized(String id, String password) {
        NodeSecurity clientSecurity = findNodeSecurity(id);
        if (clientSecurity != null && clientSecurity.getPassword() != null
                && !clientSecurity.getPassword().equals("")
                && clientSecurity.getPassword().equals(password)) {
            return true;
        }
        return false;
    }

    @SuppressWarnings("unchecked")
    public Node findIdentity() {
        List<Node> list = jdbcTemplate.query(findNodeIdentitySql,
                new NodeRowMapper());
        return (Node) getFirstEntry(list);
    }

    public List<Node> findNodesToPull() {
        return findSourceNodesFor(DataEventAction.WAIT_FOR_POLL);
    }

    public List<Node> findNodesToPushTo() {
        return findTargetNodesFor(DataEventAction.PUSH);
    }
    
    @SuppressWarnings("unchecked")
    public List<Node> findSourceNodesFor(DataEventAction eventAction) {
        Node node = findIdentity();
        return jdbcTemplate.query(findNodesWhoTargetMeSql, new Object[] {
                node.getNodeGroupId(), eventAction.getCode() },
                new NodeRowMapper());
    }
    
    @SuppressWarnings("unchecked")
    public List<Node> findTargetNodesFor(DataEventAction eventAction) {
        Node node = findIdentity();
        return jdbcTemplate.query(findNodesWhoITargetSql, new Object[] {
                node.getNodeGroupId(), eventAction.getCode() },
                new NodeRowMapper());
    }


    class NodeRowMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            Node node = new Node();
            node.setNodeId(rs.getString(1));
            node.setNodeGroupId(rs.getString(2));
            node.setExternalId(rs.getString(3));
            node.setSyncEnabled(rs.getBoolean(4));
            node.setSyncURL(rs.getString(5));
            node.setSchemaVersion(rs.getString(6));
            node.setDatabaseType(rs.getString(7));
            node.setDatabaseVersion(rs.getString(8));
            return node;
        }
    }

    class NodeSecurityRowMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            NodeSecurity clientSecurity = new NodeSecurity();
            clientSecurity.setNodeId(rs.getString(1));
            clientSecurity.setPassword(rs.getString(2));
            clientSecurity.setRegistrationEnabled(rs.getBoolean(3));
            clientSecurity.setRegistrationTime(rs.getTimestamp(4));
            return clientSecurity;
        }
    }

    public void setFindNodeSecuritySql(String findNodeSecuritySql) {
        this.findNodeSecuritySql = findNodeSecuritySql;
    }

    public void setFindNodeSql(String findNodeSql) {
        this.findNodeSql = findNodeSql;
    }

    public void setFindNodeIdentitySql(String findNodeIdentitySql) {
        this.findNodeIdentitySql = findNodeIdentitySql;
    }

    public void setFindNodesWhoITargetSql(String findNodesWhoITargetSql) {
        this.findNodesWhoITargetSql = findNodesWhoITargetSql;
    }

    public void setFindNodesWhoTargetMeSql(String findNodesWhoTargetMeSql) {
        this.findNodesWhoTargetMeSql = findNodesWhoTargetMeSql;
    }

    public void setUpdateNodeSql(String updateNodeSql) {
        this.updateNodeSql = updateNodeSql;
    }

}
