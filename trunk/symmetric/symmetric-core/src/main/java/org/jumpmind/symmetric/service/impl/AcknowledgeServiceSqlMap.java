package org.jumpmind.symmetric.service.impl;

import org.jumpmind.db.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import java.util.Map;

public class AcknowledgeServiceSqlMap extends AbstractSqlMap {

    public AcknowledgeServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);
        putSql("selectDataIdSql",
                "select data_id from $(prefixName)_data_event b where batch_id = ? order by data_id   ");
    }

}