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
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlScript;
import static org.junit.Assert.*;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

public class DatabasePlatformTest {

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
    public void testTableRebuild() throws Exception {
        Table table = new Table("TEST_REBUILD");
        table.addColumn(new Column("ID1", true));
        table.getColumnWithName("ID1").setTypeCode(Types.INTEGER);
        table.getColumnWithName("ID1").setRequired(true);
        
        table.addColumn(new Column("NOTES"));
        table.getColumnWithName("NOTES").setTypeCode(Types.VARCHAR);
        table.getColumnWithName("NOTES").setSize("20");
        table.getColumnWithName("NOTES").setDefaultValue("1234");
        
        Table origTable = (Table)table.clone();
        
        Table tableFromDatabase = dropCreateAndThenReadTable(table);
        
        assertNotNull(tableFromDatabase);
        assertEquals(2, tableFromDatabase.getColumnCount());
        
        ISqlTemplate template = platform.getSqlTemplate();
        String delimiter = platform.getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";
        assertEquals(1, template.update(String.format("insert into %s%s%s values(?,?)", delimiter, tableFromDatabase.getName(), delimiter), 1, "test"));
        
        table.addColumn(new Column("ID2", true));
        table.getColumnWithName("ID2").setTypeCode(Types.VARCHAR);
        table.getColumnWithName("ID2").setSize("20");
        table.getColumnWithName("ID2").setRequired(true);
        table.getColumnWithName("ID2").setDefaultValue("value");
        
        table.addColumn(new Column("NOTES2"));
        table.getColumnWithName("NOTES2").setTypeCode(Types.VARCHAR);
        table.getColumnWithName("NOTES2").setSize("20");
        table.getColumnWithName("NOTES2").setDefaultValue("1234");
        
        // alter to add two columns that will cause a table rebuild
        platform.alterTables(false, table);
        tableFromDatabase = platform.getTableFromCache(table.getName(), true);
        assertNotNull(tableFromDatabase);
        assertEquals(4, tableFromDatabase.getColumnCount());
        assertEquals(1, template.queryForLong(String.format("select count(*) from %s%s%s", delimiter, tableFromDatabase.getName(), delimiter)));

        // alter to remove two columns that will cause a table rebuild
        platform.alterTables(false, origTable);
        tableFromDatabase = platform.getTableFromCache(origTable.getName(), true);
        assertNotNull(tableFromDatabase);
        assertEquals(2, tableFromDatabase.getColumnCount());
        assertEquals(1, template.queryForLong(String.format("select count(*) from %s%s%s", delimiter, tableFromDatabase.getName(), delimiter)));

    }
    
    @Test
    public void testAddDefaultValueToVarcharColumn() throws Exception {
        Table table = new Table("TEST_ADD_DEFAULT");
        table.addColumn(new Column("ID1", true));
        table.getColumnWithName("ID1").setTypeCode(Types.INTEGER);
        table.getColumnWithName("ID1").setRequired(true);
        
        table.addColumn(new Column("NOTES"));
        table.getColumnWithName("NOTES").setTypeCode(Types.VARCHAR);
        table.getColumnWithName("NOTES").setSize("20");
        
        dropCreateAndThenReadTable(table);
        
        final String DEFAULT_VALUE = "SOMETHING";
        table.getColumnWithName("NOTES").setDefaultValue(DEFAULT_VALUE);
        
        platform.alterTables(false, table);
        Table tableFromDatabase = platform.getTableFromCache(table.getName(), true);
        
        assertEquals(DEFAULT_VALUE, tableFromDatabase.getColumnWithName("NOTES").getDefaultValue());
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
        
        assertEquals(table.getColumnWithName("NOTES").getDefaultValue(), 
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
        boolean upgradeSupported = !platform.getName().equals(DatabaseNamesConstants.DERBY)
                && !platform.getName().equals(DatabaseNamesConstants.HSQLDB2)
                && !platform.getName().equals(DatabaseNamesConstants.INFORMIX)
                && !platform.getName().equals(DatabaseNamesConstants.DB2)
                && !platform.getName().equals(DatabaseNamesConstants.ASE)
                && !platform.getName().equals(DatabaseNamesConstants.SQLANYWHERE);

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

            assertNotNull(tableFromDatabase);
            
            assertTrue(tableFromDatabase.getColumnWithName("ID").isPrimaryKey());

            String insertSql = "insert into \"TEST_UPGRADE\" (\"ID\",\"NOTES\") values(null,?)";
            insertSql = insertSql.replaceAll("\"", platform.getDatabaseInfo().getDelimiterToken());

            long id1 = platform.getSqlTemplate()
                    .insertWithGeneratedKey(insertSql, "ID", getSequenceName(platform),
                            new Object[] { "test" }, new int[] { Types.VARCHAR });

            table.getColumnWithName("ID").setTypeCode(Types.BIGINT);

            IDdlBuilder builder = platform.getDdlBuilder();
            String alterSql = builder.alterTable(tableFromDatabase, table);

            assertFalse(alterSql, alterSql.toLowerCase().contains("create table"));

            new SqlScript(alterSql, platform.getSqlTemplate(), true, platform.getSqlScriptReplacementTokens()).execute(true);

            tableFromDatabase = platform.getTableFromCache(table.getName(), true);

            assertEquals(Types.BIGINT, table.getColumnWithName("ID").getMappedTypeCode());
            assertTrue(tableFromDatabase.getColumnWithName("ID").isPrimaryKey());

            long id2 = platform.getSqlTemplate()
                    .insertWithGeneratedKey(insertSql, "ID", getSequenceName(platform),
                            new Object[] { "test" }, new int[] { Types.VARCHAR });

            assertNotSame(id1, id2);
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
                true);
        Table table = platform.getTableFromCache(SIMPLE_TABLE, true);
        assertNotNull("Could not find " + SIMPLE_TABLE, table);
        assertEquals("The id column was not read in as an autoincrement column", true, table
                .getColumnWithName("id").isAutoIncrement());
    }

    @Test
    public void testReadTestUppercase() throws Exception {
        Table table = platform.getTableFromCache(UPPERCASE_TABLE, true);
        assertNotNull("Could not find " + UPPERCASE_TABLE, table);
        assertEquals("The id column was not read in as an autoincrement column", true, table
                .getColumnWithName("id").isAutoIncrement());
    }

}