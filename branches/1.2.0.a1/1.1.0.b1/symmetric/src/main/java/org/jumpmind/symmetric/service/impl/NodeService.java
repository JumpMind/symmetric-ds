/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Andrew Wilcox <andrewbwilcox@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.springframework.jdbc.core.RowMapper;

public class NodeService extends AbstractService implements INodeService {

    @SuppressWarnings("unused")
    private static final Log logger = LogFactory.getLog(NodeService.class);

    private String findNodeSql;

    private String findNodeSecuritySql;

    private String findNodeIdentitySql;

    private String findNodesWhoTargetMeSql;

    private String findNodesWhoITargetSql;

    private String updateNodeSql;
    
    private String isNodeRegisteredSql;
    
    private String nodeChannelControlIgnoreSql;
    
    private String insertNodeChannelControlSql;
    
    private String findNodeByExternalIdSql;
    
    /**
     * Lookup a node in the database, which contains information for synching
     * with it.
     */
    @SuppressWarnings("unchecked")
    public Node findNode(String id) {
        List<Node> list = jdbcTemplate.query(findNodeSql, new Object[] { id },
                new NodeRowMapper());
        return (Node) getFirstEntry(list);
    }
    
    @SuppressWarnings("unchecked")
    public Node findNodeByExternalId(String nodeGroupId, String externalId) {
        List<Node> list = jdbcTemplate.query(findNodeByExternalIdSql, new Object[] { nodeGroupId, externalId },
                new NodeRowMapper());
        return (Node) getFirstEntry(list);
    }
    
    public void ignoreNodeChannelForExternalId(boolean enabled, String channelId, String nodeGroupId, String externalId) {
       Node node = findNodeByExternalId(nodeGroupId, externalId);
       if (jdbcTemplate.update(nodeChannelControlIgnoreSql, new Object[] { enabled, node.getNodeId(), channelId}) == 0) {           
           jdbcTemplate.update(insertNodeChannelControlSql, new Object[] { node.getNodeId(), channelId, enabled, false});           
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
     * Check that the given node and password match in the node_security
     * table. A node must authenticate before it's allowed to sync data.
     */
    public boolean isNodeAuthorized(String id, String password) {
        NodeSecurity nodeSecurity = findNodeSecurity(id);
        if (nodeSecurity != null
                && ((nodeSecurity.getPassword() != null && !nodeSecurity.getPassword().equals("") && nodeSecurity
                        .getPassword().equals(password)) || nodeSecurity.isRegistrationEnabled())) {
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
            node.setSymmetricVersion(rs.getString(9));
            return node;
        }
    }

    class NodeSecurityRowMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            NodeSecurity nodeSecurity = new NodeSecurity();
            nodeSecurity.setNodeId(rs.getString(1));
            nodeSecurity.setPassword(rs.getString(2));
            nodeSecurity.setRegistrationEnabled(rs.getBoolean(3));
            nodeSecurity.setRegistrationTime(rs.getTimestamp(4));
            return nodeSecurity;
        }
    }
    
    public boolean isExternalIdRegistered(String nodeGroupId, String externalId) {
        return jdbcTemplate.queryForInt(isNodeRegisteredSql, new Object[] {nodeGroupId, externalId}) > 0;
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

    public void setIsNodeRegisteredSql(String isNodeRegisteredSql) {
        this.isNodeRegisteredSql = isNodeRegisteredSql;
    }

    public void setNodeChannelControlIgnoreSql(String nodeChannelControlIgnoreSql) {
        this.nodeChannelControlIgnoreSql = nodeChannelControlIgnoreSql;
    }

    public void setInsertNodeChannelControlSql(String insertNodeChannelControlSql) {
        this.insertNodeChannelControlSql = insertNodeChannelControlSql;
    }

    public void setFindNodeByExternalIdSql(String findNodeByExternalIdSql) {
        this.findNodeByExternalIdSql = findNodeByExternalIdSql;
    }

}
