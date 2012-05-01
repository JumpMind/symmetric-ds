package org.jumpmind.db;

import java.io.InputStreamReader;

import junit.framework.Assert;

import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabasePlatformTest extends AbstractDbTest {

    private static IDatabasePlatform platform;

    @BeforeClass
    public static void setup() throws Exception {
        platform = DbTestUtils.createDatabasePlatform(DbTestUtils.ROOT);
    }

    @Test
    public void testCreateDatabase() throws Exception {
        platform.createDatabase(new DatabaseIO().read(new InputStreamReader(
                DatabasePlatformTest.class.getResourceAsStream("/testCreateDatabase.xml"))), true,
                false);
        
        Table table = platform.getTableFromCache("test_simple_table", true);
        Assert.assertNotNull(table);
        Assert.assertEquals(true, table.getColumnWithName("id").isAutoIncrement());
    }
}
