/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.db;

import java.io.InputStreamReader;
import java.sql.Types;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlScript;
import org.junit.Assert;
import org.junit.Before;
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

    @Before
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
    public void testExportDefaultValueWithUnderscores() {
        Table table = new Table("TEST_DEFAULT_UNDERSCORES");
        table.addColumn(new Column("ID", true));
        table.getColumnWithName("ID").setTypeCode(Types.INTEGER);
        table.getColumnWithName("ID").setRequired(true);
        table.addColumn(new Column("NOTES"));
        table.getColumnWithName("NOTES").setTypeCode(Types.VARCHAR);
        table.getColumnWithName("NOTES").setSize("20");
        table.getColumnWithName("NOTES").setDefaultValue("this_has_underscores");
        
        Table tableFromDatabase = dropCreateAndThenReadTable(table);
        
        Assert.assertEquals(table.getColumnWithName("NOTES").getDefaultValue(), 
                tableFromDatabase.getColumnWithName("NOTES").getDefaultValue());
        
    }
    
    protected Table dropCreateAndThenReadTable(Table table) {
        Database database = new Database();
        database.addTable(table);        
        platform.createDatabase(database, true, false);        
        return platform.getTableFromCache(table.getName(), true);
    
    }

    @Test
    public void testUpgradePrimaryKeyAutoIncrementFromIntToBigInt() throws Exception {
        boolean upgradeSupported = platform.getName() != DatabaseNamesConstants.DERBY &&
                platform.getName() != DatabaseNamesConstants.HSQLDB2 && 
                        platform.getName() != DatabaseNamesConstants.INFORMIX && 
                        platform.getName() != DatabaseNamesConstants.DB2;

        if (upgradeSupported) {
            Table table = new Table("TEST_UPGRADE");
            table.addColumn(new Column("ID", true));
            table.getColumnWithName("ID").setTypeCode(Types.INTEGER);
            table.getColumnWithName("ID").setAutoIncrement(true);
            table.getColumnWithName("ID").setRequired(true);
            table.addColumn(new Column("NOTES"));
            table.getColumnWithName("NOTES").setTypeCode(Types.VARCHAR);
            table.getColumnWithName("NOTES").setSize("100");

            Table tableFromDatabase = dropCreateAndThenReadTable(table);

            Assert.assertNotNull(tableFromDatabase);
            
            Assert.assertTrue(tableFromDatabase.getColumnWithName("ID").isPrimaryKey());

            String insertSql = "insert into \"TEST_UPGRADE\" (\"ID\",\"NOTES\") values(null,?)";
            insertSql = insertSql.replaceAll("\"", platform.getDatabaseInfo().getDelimiterToken());

            long id1 = platform.getSqlTemplate()
                    .insertWithGeneratedKey(insertSql, "ID", getSequenceName(platform),
                            new Object[] { "test" }, new int[] { Types.VARCHAR });

            table.getColumnWithName("ID").setTypeCode(Types.BIGINT);

            IDdlBuilder builder = platform.getDdlBuilder();
            String alterSql = builder.alterTable(tableFromDatabase, table);

            Assert.assertFalse(alterSql, alterSql.toLowerCase().contains("create table"));

            new SqlScript(alterSql, platform.getSqlTemplate(), true, platform.getSqlScriptReplacementTokens()).execute(true);

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
        platform.createDatabase(DatabaseXmlUtil.read(new InputStreamReader(
                DatabasePlatformTest.class.getResourceAsStream("/testCreateDatabase.xml"))), true,
                false);
        Table table = platform.getTableFromCache(SIMPLE_TABLE, true);
        Assert.assertNotNull("Could not find " + SIMPLE_TABLE, table);
        Assert.assertEquals("The id column was not read in as an autoincrement column", true, table
                .getColumnWithName("id").isAutoIncrement());
    }
    
    @Test 
    public void testPostgresCreateAndReadNumericType() throws Exception {
        if (platform.getName().equals(DatabaseNamesConstants.POSTGRESQL)) {
           Table table = new Table("with_numeric");
           table.addColumn(new Column("id", true, Types.DECIMAL, 0, 0));
           platform.createTables(true, true, table);
           
           Table fromDatabase = platform.readTableFromDatabase(null, null, table.getName());
           
           Assert.assertNotNull(fromDatabase);
           Assert.assertEquals(table.getName(), fromDatabase.getName());
           Assert.assertEquals(Types.DECIMAL, fromDatabase.getColumn(0).getMappedTypeCode());
           
           Assert.assertEquals(DatabaseXmlUtil.toXml(table), DatabaseXmlUtil.toXml(fromDatabase));
        }
    }

    @Test
    public void testReadTestUppercase() throws Exception {
        Table table = platform.getTableFromCache(UPPERCASE_TABLE, true);
        Assert.assertNotNull("Could not find " + UPPERCASE_TABLE, table);
        Assert.assertEquals("The id column was not read in as an autoincrement column", true, table
                .getColumnWithName("id").isAutoIncrement());
    }

}