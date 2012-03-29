package org.jumpmind.symmetric.core.db;

import org.jumpmind.symmetric.core.db.mapper.StringMapper;

abstract public class SqlConstants {

    public static final String ALWAYS_TRUE_CONDITION = "1=1";

    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;

    public static final int DEFAULT_STREAMING_FETCH_SIZE = 1000;
    
    public static final StringMapper STRING_MAPPER = new StringMapper();
}
