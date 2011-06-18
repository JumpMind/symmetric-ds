package org.jumpmind.symmetric.core.db.mapper;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.NodeSecurity;


public class NodeSecurityMapper implements ISqlRowMapper<NodeSecurity> {
    
    public NodeSecurity mapRow(Row row) {
        NodeSecurity nodeSecurity = new NodeSecurity();
        nodeSecurity.setNodeId(row.getString("NODE_ID"));
        nodeSecurity.setNodePassword(row.getString("NODE_PASSWORD"));
        nodeSecurity.setRegistrationEnabled(row.getBoolean("REGISTRATION_ENABLED"));
        nodeSecurity.setRegistrationTime(row.getDateTime("REGISTRATION_TIME"));
        nodeSecurity.setInitialLoadEnabled(row.getBoolean("INITIAL_LOAD_ENABLED"));
        nodeSecurity.setInitialLoadTime(row.getDateTime("INITIAL_LOAD_TIME"));
        nodeSecurity.setCreatedAtNodeId(row.getString("CREATED_AT_NODE_ID"));
        return nodeSecurity;
    }
}