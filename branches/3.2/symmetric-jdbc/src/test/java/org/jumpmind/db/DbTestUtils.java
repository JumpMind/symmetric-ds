package org.jumpmind.db;

import java.io.File;

import org.apache.commons.io.FileUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.JdbcDatabasePlatformFactory;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.util.BasicDataSourceFactory;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.security.SecurityService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class DbTestUtils {

    Logger logger = LoggerFactory.getLogger(getClass());

    public final static String DB_TEST_PROPERTIES = "/db-test.properties";
    public static final String ROOT = "root";
    public static final String CLIENT = "client";

    public static IDatabasePlatform createDatabasePlatform(String name) throws Exception {
        File f = new File(String.format("target/%sdbs", name));
        FileUtils.deleteDirectory(f);
        f.mkdir();
        EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(
                DatabasePlatformTest.class.getResource(DB_TEST_PROPERTIES), String.format(
                        "test.%s", name), name);
        return JdbcDatabasePlatformFactory.createNewPlatformInstance(
                BasicDataSourceFactory.create(properties, new SecurityService()),
                new SqlTemplateSettings(), true);
    }

}
