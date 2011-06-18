package org.jumpmind.symmetric.core.db.mapper;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.Node;

public class NodeMapper implements ISqlRowMapper<Node> {

    public Node mapRow(Row row) {
        Node node = new Node();
        node.setNodeId(row.getString("NODE_ID"));
        node.setNodeGroupId(row.getString("NODE_GROUP_ID"));
        node.setExternalId(row.getString("EXTERNAL_ID"));
        node.setSyncEnabled(row.getBoolean("SYNC_ENABLED"));
        node.setSyncUrl(row.getString("SYNC_URL"));
        node.setSchemaVersion(row.getString("SCHEMA_VERSION"));
        node.setDatabaseType(row.getString("DATABASE_TYPE"));
        node.setDatabaseVersion(row.getString("DATABASE_VERSION"));
        node.setSymmetricVersion(row.getString("SYMMETRIC_VERSION"));
        node.setCreatedAtNodeId(row.getString("CREATED_AT_NODE_ID"));
        node.setHeartbeatTime(row.getDateTime("HEARTBEAT_TIME"));
        node.setTimezoneOffset(row.getString("TIMEZONE_OFFSET"));
        node.setBatchToSendCount(row.getInt("BATCH_TO_SEND_COUNT"));
        node.setBatchInErrorCount(row.getInt("BATCH_IN_ERROR_COUNT"));
        node.setDeploymentType(row.getString("DEPLOYMENT_TYPE"));
        return node;
    }
    
}
