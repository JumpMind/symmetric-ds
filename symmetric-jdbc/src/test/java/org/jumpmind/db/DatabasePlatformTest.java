package org.jumpmind.db;

import java.io.InputStreamReader;
import java.sql.Types;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlScript;
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

    // @Before
    public void turnOnDebug() {
        Logger logger = Logger.getLogger("org.jumpmind.db");
        originalLevel = logger.getLevel();
        logger.setLevel(Level.TRACE);
    }

    // @After
    public void turnOffDebug() {
        Logger logger = Logger.getLogger("org.jumpmind.db");
        logger.setLevel(originalLevel);
    }

    @Test
    public void testUpgradeFromIntToBigInt() throws Exception {
        boolean upgradeSupported = platform.getName() != DatabaseNamesConstants.DERBY &&
                platform.getName() != DatabaseNamesConstants.HSQLDB2 && 
                        platform.getName() != DatabaseNamesConstants.INFORMIX;

        if (upgradeSupported) {
            Table table = new Table("TEST_UPGRADE");
            table.addColumn(new Column("ID", true));
            table.getColumnWithName("ID").setTypeCode(Types.INTEGER);
            table.getColumnWithName("ID").setAutoIncrement(true);
            table.getColumnWithName("ID").setRequired(true);
            table.addColumn(new Column("NOTES"));
            table.getColumnWithName("NOTES").setTypeCode(Types.VARCHAR);
            table.getColumnWithName("NOTES").setSize("100");

            Database database = new Database();
            database.addTable(table);

            platform.createDatabase(database, true, false);

            Table tableFromDatabase = platform.getTableFromCache(table.getName(), true);
            Database databaseFromDatabase = new Database();
            databaseFromDatabase.addTable(tableFromDatabase);

            Assert.assertTrue(tableFromDatabase.getColumnWithName("ID").isPrimaryKey());

            Assert.assertNotNull(tableFromDatabase);

            String insertSql = "insert into \"TEST_UPGRADE\" (\"ID\",\"NOTES\") values(null,?)";
            insertSql = insertSql.replaceAll("\"", platform.getDatabaseInfo().getDelimiterToken());

            long id1 = platform.getSqlTemplate()
                    .insertWithGeneratedKey(insertSql, "ID", getSequenceName(platform),
                            new Object[] { "test" }, new int[] { Types.VARCHAR });

            table.getColumnWithName("ID").setTypeCode(Types.BIGINT);

            IDdlBuilder builder = platform.getDdlBuilder();
            String alterSql = builder.alterDatabase(databaseFromDatabase, database);

            Logger logger = Logger.getLogger("org.jumpmind.db");
            logger.info(alterSql);

            Assert.assertFalse(alterSql, alterSql.toLowerCase().contains("create table"));

            new SqlScript(alterSql, platform.getSqlTemplate(), true).execute(true);

            tableFromDatabase = platform.getTableFromCache(table.getName(), true);

            Assert.assertEquals(Types.BIGINT, table.getColumnWithName("ID").getMappedTypeCode());
            Assert.assertTrue(tableFromDatabase.getColumnWithName("ID").isPrimaryKey());

            long id2 = platform.getSqlTemplate()
                    .insertWithGeneratedKey(insertSql, "ID", getSequenceName(platform),
                            new Object[] { "test" }, new int[] { Types.VARCHAR });

            Assert.assertNotSame(id1, id2);
        }
    }

    protected String getSequenceName(IDatabasePlatform platform) {
        if (platform.getName().equals(DatabaseNamesConstants.ORACLE)) {
            return "TEST_UPGRADE_ID";
        } else if (platform.getName().equals(DatabaseNamesConstants.INTERBASE)) {
            return "SEQ_TEST_UPGRADE_ID";
        } else {
            return "test_upgrade_id";
        }
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
