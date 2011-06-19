package org.jumpmind.symmetric.core.db.mapper;

import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.NodeChannelControl;

public class NodeChannelControlMapper implements ISqlRowMapper<NodeChannelControl> {

    public NodeChannelControl mapRow(Row row) {     
        NodeChannelControl control = new NodeChannelControl();
        control.setChannelId(row.getString("CHANNEL_ID"));
        control.setIgnoreEnabled(row.getBoolean("IGNORE_ENABLED"));
        control.setLastExtractTime(row.getDateTime("LAST_EXTRACT_TIME"));
        control.setNodeId(row.getString("NODE_ID"));
        control.setSuspendEnabled(row.getBoolean("SUSPEND_ENABLED"));
        return control;
    }
}
