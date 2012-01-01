package org.jumpmind.symmetric.service.impl;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.AbstractSqlMap;
import java.util.Map;

public class DataExtractorServiceSqlMap extends AbstractSqlMap {

    public DataExtractorServiceSqlMap(IDatabasePlatform platform,
            Map<String, String> replacementTokens) {
        super(platform, replacementTokens);

    }

}