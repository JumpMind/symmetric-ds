package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class DataExtractorSqlMap extends AbstractSqlMap {

    public DataExtractorSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);
        
        // @formatter:off
        putSql("selectNodeIdsForExtractSql", "select distinct(node_id) from $(extract_request) where status=?");
        
        putSql("selectExtractRequestForNodeSql", "select * from $(extract_request) where node_id=? and status=?");
        
        putSql("insertExtractRequestSql", "insert into $(extract_request) (request_id, node_id, status, start_batch_id, end_batch_id, trigger_id, router_id, last_update_time, create_time) values(null, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp)");
        
        putSql("updateExtractRequestStatus", "update $(extract_request) set status=? where request_id=?");
        
        putSql("resetExtractRequestStatus", "update $(extract_request) set status=? where start_batch_id >= ? and end_batch_id <= ? and node_id=?");
    }

}
