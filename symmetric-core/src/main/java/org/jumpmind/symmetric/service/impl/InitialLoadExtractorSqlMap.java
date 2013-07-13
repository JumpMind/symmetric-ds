package org.jumpmind.symmetric.service.impl;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;

public class InitialLoadExtractorSqlMap extends AbstractSqlMap {

    public InitialLoadExtractorSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);
        // @formatter:off
        putSql("selectNodeIdsForExtractSql", "select distinct(node_id) from $(extract_request) where status=?");
        
        putSql("selectExtractRequestForNodeSql", "select * from $(extract_request) where node_id=? and status=?");
    }

}
