package org.jumpmind.db;

import java.io.InputStreamReader;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabasePlatformTest extends AbstractDbTest {

    private static IDatabasePlatform platform;

    protected final static String SIMPLE_TABLE = "test_simple_table";

    protected final static String UPPERCASE_TABLE = "TEST_UPPERCASE_TABLE";

    protected Level originalLevel;

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
    }

    //@Before
    public void turnOnDebug() {
        Logger logger = Logger.getLogger("org.jumpmind.db");
        originalLevel = logger.getLevel();
        logger.setLevel(Level.TRACE);
    }

    //@After
    public void turnOffDebug() {
        Logger logger = Logger.getLogger("org.jumpmind.db");
        logger.setLevel(originalLevel);
    }

    @Test
    public void testCreateAndReadTestSimpleTable() throws Exception {
        platform.createDatabase(new DatabaseIO().read(new InputStreamReader(
                DatabasePlatformTest.class.getResourceAsStream("/testCreateDatabase.xml"))), true,
                false);
        Table table = platform.getTableFromCache(SIMPLE_TABLE, true);
        Assert.assertNotNull("Could not find " + SIMPLE_TABLE, table);
        Assert.assertEquals("The id column was not read in as an autoincrement column", true, table
                .getColumnWithName("id").isAutoIncrement());
    }

    @Test
    public void testReadTestUppercase() throws Exception {
        Table table = platform.getTableFromCache(UPPERCASE_TABLE, true);
        Assert.assertNotNull("Could not find " + UPPERCASE_TABLE, table);
        Assert.assertEquals("The id column was not read in as an autoincrement column", true, table
                .getColumnWithName("id").isAutoIncrement());
    }
}
