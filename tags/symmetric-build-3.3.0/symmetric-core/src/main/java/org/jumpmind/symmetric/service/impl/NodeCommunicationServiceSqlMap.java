package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class NodeCommunicationServiceSqlMap extends AbstractSqlMap {

    public NodeCommunicationServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);
        
        putSql("clearLocksOnRestartSql", "update $(node_communication) set lock_time=null where locking_server_id=? and lock_time is not null");

        putSql("selectNodeCommunicationSql",
                "select * from $(node_communication) where communication_type=? order by last_lock_time");

        putSql("insertNodeCommunicationSql", "insert into $(node_communication) ("
                + "lock_time,locking_server_id,last_lock_millis,success_count,fail_count,"
                + "total_success_count,total_fail_count,total_success_millis,total_fail_millis,last_lock_time,"
                + "node_id,communication_type) values(?,?,?,?,?,?,?,?,?,?,?,?)");

        putSql("updateNodeCommunicationSql",
                "update $(node_communication) set lock_time=?,locking_server_id=?,last_lock_millis=?,"
                        + "success_count=?,fail_count=?,total_success_count=?,total_fail_count=?,"
                        + "total_success_millis=?,total_fail_millis=?, last_lock_time=? "
                        + "where node_id=? and communication_type=?");

        putSql("deleteNodeCommunicationSql",
                "delete from $(node_communication) where node_id=? and communication_type=?");

        putSql("aquireLockSql",
                "update $(node_communication) set locking_server_id=?, lock_time=?, last_lock_time=? where "
                        + "  node_id=? and communication_type=? and "
                        + " (lock_time is null or lock_time < ? or locking_server_id=?)   ");

    }

}