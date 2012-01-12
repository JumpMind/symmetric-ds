package org.jumpmind.db;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.db.platform.DatabasePlatformSettings;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.util.DataSourceProperties;

abstract public class DbTestUtils {

    public static final String ROOT = "root";
    public static final String CLIENT = "client";

    public static IDatabasePlatform createDatabasePlatform(String name) throws Exception {
        FileUtils.deleteDirectory(new File(String.format("target/%sdbs", name)));
        DataSourceProperties properties = new DataSourceProperties(String.format("test.%s", name),
                DatabasePlatformTest.class.getResource("/test-db.properties"), name);
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(properties.getDataSource(), new DatabasePlatformSettings());
    }

}
