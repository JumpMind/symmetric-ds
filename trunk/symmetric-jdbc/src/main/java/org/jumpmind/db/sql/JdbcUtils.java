package org.jumpmind.db.sql;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

abstract public class JdbcUtils {
    
    private static Logger log = LoggerFactory.getLogger(JdbcUtils.class);

    private JdbcUtils() {
    }    
    
    public static NativeJdbcExtractor getNativeJdbcExtractory () {
        try {
            return (NativeJdbcExtractor) Class
                    .forName(
                            System.getProperty("db.native.extractor",
                                    "org.springframework.jdbc.support.nativejdbc.CommonsDbcpNativeJdbcExtractor"))
                    .newInstance();
        } catch (Exception ex) {
            log.error("The native jdbc extractor has not been configured.  Defaulting to the common basic datasource extractor.", ex);
            return new CommonsDbcpNativeJdbcExtractor();
        }
    }

}
