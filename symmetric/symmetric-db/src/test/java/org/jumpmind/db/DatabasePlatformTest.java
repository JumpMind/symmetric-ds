package org.jumpmind.db;

import java.io.File;
import java.io.InputStreamReader;

import org.apache.commons.io.FileUtils;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.util.DataSourceProperties;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabasePlatformTest extends AbstractDbTest {

    private static IDatabasePlatform platform;

    @BeforeClass
    public static void setup() throws Exception {        
        FileUtils.deleteDirectory(new File("target/dbs"));
        DataSourceProperties properties = new DataSourceProperties(
                DatabasePlatformTest.class.getResourceAsStream("/test-db.properties"), "root");
        platform = JdbcDatabasePlatformFactory
                .createNewPlatformInstance(properties.getDataSource());
    }

    @Test
    public void testCreateDatabase() throws Exception {
        platform.createDatabase(new DatabaseIO().read(new InputStreamReader(
                DatabasePlatformTest.class.getResourceAsStream("/testCreateDatabase.xml"))), true,
                false);
    }
}
