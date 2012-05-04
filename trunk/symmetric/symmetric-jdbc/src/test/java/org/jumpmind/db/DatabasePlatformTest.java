package org.jumpmind.db;

import java.io.InputStreamReader;

import junit.framework.Assert;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabasePlatformTest extends AbstractDbTest {

    private static IDatabasePlatform platform;

    protected final static String SIMPLE_TABLE = "test_simple_table";

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
    }

    @Test
    public void testCreateAndReadTestSimpleTable() throws Exception {

        Logger logger = Logger.getLogger("org.jumpmind.db");
        Level origLevel = logger.getLevel();
        try {
            logger.setLevel(Level.TRACE);

            platform.createDatabase(new DatabaseIO().read(new InputStreamReader(
                    DatabasePlatformTest.class.getResourceAsStream("/testCreateDatabase.xml"))),
                    true, false);
            Table table = platform.getTableFromCache(SIMPLE_TABLE, true);
            Assert.assertNotNull("Could not find " + SIMPLE_TABLE, table);
            Assert.assertEquals("The id column was not read in as an autoincrement column", true,
                    table.getColumnWithName("id").isAutoIncrement());
            
        } finally {
            logger.setLevel(origLevel);
        }
    }
}
