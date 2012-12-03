package org.jumpmind.db.sql;

import org.apache.commons.lang.time.FastDateFormat;
import org.jumpmind.db.sql.mapper.StringMapper;

abstract public class SqlConstants {

    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S",
            "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd HH:mm:ss.SSS",  "yyyy-MM-dd HH:mm", "yyyy-MM-dd" };

    public static final String[] TIME_PATTERNS = { "HH:mm:ss.S", "HH:mm:ss",
            "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss.SSS", "yyyy-MM-dd HH:mm:ss" };

    public static final FastDateFormat JDBC_TIMESTAMP_FORMATTER = FastDateFormat
            .getInstance("yyyy-MM-dd hh:mm:ss.SSS");

    public static final String ALWAYS_TRUE_CONDITION = "1=1";

    public static final int DEFAULT_QUERY_TIMEOUT_SECONDS = 300;

    public static final int DEFAULT_STREAMING_FETCH_SIZE = 1000;

    public static final StringMapper STRING_MAPPER = new StringMapper();
}
