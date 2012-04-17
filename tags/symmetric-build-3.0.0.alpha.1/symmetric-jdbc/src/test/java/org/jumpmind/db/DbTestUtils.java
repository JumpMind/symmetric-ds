package org.jumpmind.db;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.util.DataSourceProperties;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class DbTestUtils {
    
    Logger logger = LoggerFactory.getLogger(getClass());
    
    public final static String DB_TEST_PROPERTIES = "/db-test.properties";
    public static final String ROOT = "root";
    public static final String CLIENT = "client";

    public static IDatabasePlatform createDatabasePlatform(String name) throws Exception {        
        FileUtils.deleteDirectory(new File(String.format("target/%sdbs", name)));
        DataSourceProperties properties = new DataSourceProperties(String.format("test.%s", name),
                DatabasePlatformTest.class.getResource(DB_TEST_PROPERTIES), name);
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(properties.getDataSource(), new DatabasePlatformSettings());
    }

}
