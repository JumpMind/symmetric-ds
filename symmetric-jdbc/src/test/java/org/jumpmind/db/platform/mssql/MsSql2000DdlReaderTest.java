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
package org.jumpmind.db.platform.mssql;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.regex.Pattern;

import javax.sql.DataSource;

import org.jumpmind.db.DdlReaderTestConstants;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.IndexColumn;
import org.jumpmind.db.model.NonUniqueIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class MsSql2000DdlReaderTest {
    protected MsSql2000DatabasePlatform platform;
    protected Pattern mssql2000IsoDatePattern;
    /*
     * The regular expression pattern for the mssql2000 conversion of ISO times.
     */
    protected Pattern mssql2000IsoTimePattern;
    /*
     * The regular expression pattern for the mssql2000 conversion of ISO timestamps.
     */
    protected Pattern mssql2000IsoTimestampPattern;
    ISqlTemplate sqlTemplate;
    ISqlTransaction sqlTransaction;
    protected AbstractJdbcDdlReader abstractJdbcDdlReader;
    ThreadLocalRandom rand = ThreadLocalRandom.current();

    @BeforeEach
    public void setUp() throws Exception {
        platform = mock(MsSql2000DatabasePlatform.class);
        sqlTemplate = mock(ISqlTemplate.class);
        mssql2000IsoDatePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD'\\)");
        mssql2000IsoTimePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'HH24:MI:SS'\\)");
        mssql2000IsoTimestampPattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD HH24:MI:SS'\\)");
    }

    @Test
    void testMsSqlDdlReaderConstructor() throws Exception {
        MsSqlDdlReader testReader = new MsSqlDdlReader(platform);
        testReader.setDefaultCatalogPattern(null);
        testReader.setDefaultSchemaPattern(null);
        testReader.setDefaultTablePattern("%");
        assertEquals(null, testReader.getDefaultCatalogPattern());
        assertEquals(null, testReader.getDefaultSchemaPattern());
        assertEquals("%", testReader.getDefaultTablePattern());
        assertEquals(true, mssql2000IsoDatePattern.pattern().equals("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD'\\)"));
        assertEquals(true, mssql2000IsoTimePattern.pattern().equals("TO_DATE\\('([^']*)'\\, 'HH24:MI:SS'\\)"));
        assertEquals(true, mssql2000IsoTimestampPattern.pattern().equals("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD HH24:MI:SS'\\)"));
    }

    @Test
    void testGetTableNames() throws Exception {
        MsSqlDdlReader testReader = new MsSqlDdlReader(platform);
        List<String> tableNameResults = new ArrayList<String>();
        List<String> tableNames = new ArrayList<String>();
        tableNames.add("testName");
        MsSqlJdbcSqlTemplate testTemplate = mock(MsSqlJdbcSqlTemplate.class);
        when(platform.getSqlTemplateDirty()).thenReturn(testTemplate);
        doReturn(tableNames).when(testTemplate).queryWithHandler(ArgumentMatchers.anyString(),
                ArgumentMatchers.any(), ArgumentMatchers.any(), ArgumentMatchers.any());
        tableNameResults = testReader.getTableNames("testCatlog", "testSchema", null);
        assertEquals(tableNames, tableNameResults);
    }

    @ParameterizedTest
    @CsvSource({ "INSERT,1,0,0", "UPDATE,0,1,0", "DELETE,0,0,1", })
    void testGetTriggers(String triggerTypeParam, String isInsert, String isUpdate, String isDelete) throws Exception {
        // Mocked components
        SqlTemplateSettings settings = mock(SqlTemplateSettings.class);
        PreparedStatement ps = mock(PreparedStatement.class);
        Statement st = mock(Statement.class);
        PreparedStatement ps2 = mock(PreparedStatement.class);
        DataSource dataSource = mock(DataSource.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);
        Connection connection = mock(Connection.class);
        ResultSet rs = mock(ResultSet.class);
        ResultSet rs2 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        // "Real" components
        MsSqlDdlReader testReader = new MsSqlDdlReader(platform);
        List<Trigger> actualTriggers = new ArrayList<Trigger>();
        MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings, databaseInfo);
        // Spied components
        MsSqlJdbcSqlTemplate spyTemplate = Mockito.spy(testTemplate);
        when(platform.getSqlTemplateDirty()).thenReturn(spyTemplate);
        when(spyTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.createStatement(spyTemplate.getSettings().getResultSetType(), ResultSet.CONCUR_READ_ONLY)).thenReturn(st);
        when(connection.prepareStatement(ArgumentMatchers.anyString(), ArgumentMatchers.anyInt(), ArgumentMatchers.anyInt())).thenReturn(ps)
                .thenReturn(ps2);
        when(ps.executeQuery()).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(10);
        when(rsMetaData.getColumnLabel(1)).thenReturn("name");
        when(rs.getObject(1)).thenReturn("testTrigger");
        when(rsMetaData.getColumnLabel(2)).thenReturn("table_schema");
        when(rs.getObject(2)).thenReturn("testSchema");
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getObject(3)).thenReturn("testTableName");
        when(rsMetaData.getColumnLabel(4)).thenReturn("is_disabled");
        when(rs.getObject(4)).thenReturn("FALSE");
        when(rsMetaData.getColumnLabel(5)).thenReturn("trigger_source");
        when(rs.getObject(5)).thenReturn("create ");
        when(rsMetaData.getColumnLabel(6)).thenReturn("isupdate");
        when(rs.getObject(6)).thenReturn(isUpdate);
        when(rsMetaData.getColumnLabel(7)).thenReturn("isdelete");
        when(rs.getObject(7)).thenReturn(isDelete);
        when(rsMetaData.getColumnLabel(8)).thenReturn("isinsert");
        when(rs.getObject(8)).thenReturn(isInsert);
        when(rsMetaData.getColumnLabel(9)).thenReturn("isafter");
        when(rs.getObject(9)).thenReturn("0");
        when(rsMetaData.getColumnLabel(10)).thenReturn("isinsteadof");
        when(rs.getObject(10)).thenReturn("0");
        when(ps2.executeQuery()).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(1);
        when(rsMetaData2.getColumnLabel(1)).thenReturn("TEXT");
        when(rs2.getObject(1)).thenReturn("testText");
        Row triggerMetaData = new Row(5);
        triggerMetaData.put("trigger_name", "testTrigger");
        triggerMetaData.put("trigger_schema", "testSchema");
        triggerMetaData.put(DdlReaderTestConstants.TABLE_NAME, "testTableName");
        triggerMetaData.put("STATUS", "ACTIVE");
        triggerMetaData.put("TRIGGERING_EVENT", triggerTypeParam);
        Trigger trigger = new Trigger();
        trigger.setName("testTrigger");
        trigger.setSchemaName("testSchema");
        trigger.setTableName("testTableName");
        trigger.setEnabled(false);
        trigger.setSource("create ");
        trigger.setMetaData(triggerMetaData);
        String triggerType = triggerTypeParam;
        trigger.setTriggerType(TriggerType.valueOf(triggerType));
        actualTriggers.add(trigger);
        List<Trigger> triggers = testReader.getTriggers("test", "test", "test");
        Trigger testTrigger = triggers.get(0);
        assertEquals("testTrigger", testTrigger.getName());
        assertEquals("testSchema", testTrigger.getSchemaName());
        assertEquals("testTableName", testTrigger.getTableName());
        assertEquals(true, testTrigger.isEnabled());
        assertEquals("create ", testTrigger.getSource());
        assertEquals(true, testTrigger.getTriggerType().toString().equals(triggerTypeParam));
    }

    @ParameterizedTest
    @CsvSource({ "2008-11-11, DATE, " + Types.DATE + ",," + -1 + "", "2008-11-11 12:10:30, TIMESTAMP, " + Types.TIMESTAMP + ",," + -1 + "",
            "testDef, VARCHAR, " + Types.VARCHAR + ",254," + 254 + "", })
    void testReadTableWithBasicArgs(String defaultValue, String jdbcTypeName, int jdbcTypeCode, String testColumnSize, int platformColumnSize)
            throws Exception {
        // Mocked Components
        Connection connection = mock(Connection.class);
        DataSource dataSource = mock(DataSource.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);
        SqlTemplateSettings settings = mock(SqlTemplateSettings.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        // "Real" Components
        MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings, databaseInfo);
        MsSql2000DatabasePlatform platform = new MsSql2000DatabasePlatform(dataSource, settings);
        // Spied Components
        MsSql2000DatabasePlatform spyPlatform = Mockito.spy(platform);
        testTemplate.setIsolationLevel(1);
        MsSqlJdbcSqlTemplate spyTemplate = Mockito.spy(testTemplate);
        spyTemplate.setIsolationLevel(1);
        MsSqlDdlReader testReader = new MsSqlDdlReader(spyPlatform);
        MsSqlDdlReader spyReader = Mockito.spy(testReader);
        // Result Set Mocks
        ResultSet rs = mock(ResultSet.class);
        ResultSet rs2 = mock(ResultSet.class);
        ResultSet rs3 = mock(ResultSet.class);
        ResultSet rs4 = mock(ResultSet.class);
        ResultSet rs5 = mock(ResultSet.class);
        ResultSet stmtrs1 = mock(ResultSet.class);
        ResultSet stmtrs2 = mock(ResultSet.class);
        ResultSet stmtrs3 = mock(ResultSet.class);
        ResultSet stmtrs4 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData3 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData4 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData5 = mock(ResultSetMetaData.class);
        PreparedStatement stmt1 = mock(PreparedStatement.class);
        PreparedStatement stmt2 = mock(PreparedStatement.class);
        PreparedStatement stmt3 = mock(PreparedStatement.class);
        PreparedStatement stmt4 = mock(PreparedStatement.class);
        ResultSetMetaData stmt1RsMetaData = mock(ResultSetMetaData.class);
        when(spyTemplate.getDataSource().getConnection()).thenReturn(connection);
        doReturn(spyTemplate).when(spyPlatform).createSqlTemplate();
        when(spyPlatform.createSqlTemplate()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplateDirty()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplate()).thenReturn(spyTemplate);
        when(spyTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                (String[]) ArgumentMatchers.any())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(5);
        when(rsMetaData.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData.getColumnName(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rsMetaData.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rs.getString(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rsMetaData.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rs.getString(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        when(rsMetaData.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rsMetaData.getColumnName(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rs.getString(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        when(rsMetaData.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rsMetaData.getColumnName(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rs.getString(5)).thenReturn(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        when(metaData.getColumns(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(7);
        when(rsMetaData2.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rsMetaData2.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rs2.getString(1)).thenReturn(defaultValue);// Variable 1
        when(rsMetaData2.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rsMetaData2.getColumnName(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rs2.getString(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData2.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs2.getString(3)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData2.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData2.getColumnName(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs2.getString(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rsMetaData2.getColumnName(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rs2.getString(5)).thenReturn(jdbcTypeName);// Variable2
        when(rsMetaData2.getColumnLabel(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rsMetaData2.getColumnName(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rs2.getInt(6)).thenReturn(jdbcTypeCode);// Variable3
        when(rsMetaData2.getColumnLabel(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rsMetaData2.getColumnName(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rs2.getString(7)).thenReturn("TRUE");
        when(metaData.getImportedKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs3);
        when(rs3.next()).thenReturn(false);
        when(rs3.getMetaData()).thenReturn(rsMetaData3);
        when(rsMetaData3.getColumnCount()).thenReturn(0);
        // when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3)
        // .thenReturn(stmt4);
        // THIS SECTION IS NEW. This is because the msSqlDdlReader uses the
        // determineAutoIncrementFromResultSetMetaData
        // method, and this changes several things when testing
        when(connection.createStatement()).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3).thenReturn(stmt4);
        when(stmt1.executeQuery(ArgumentMatchers.anyString())).thenReturn(stmtrs1);
        when(stmtrs1.getMetaData()).thenReturn(stmt1RsMetaData);
        when(stmt1RsMetaData.isAutoIncrement(ArgumentMatchers.anyInt())).thenReturn(true);
        when(stmtrs1.next()).thenReturn(true).thenReturn(false);
        when(stmtrs1.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAMECAPS);
        when(stmt2.executeQuery()).thenReturn(stmtrs2);
        when(stmtrs2.next()).thenReturn(true).thenReturn(false);
        when(stmtrs2.getString(1)).thenReturn("NOTRIGHTNAME");
        when(stmtrs2.getString(2)).thenReturn("TESTSCHEMA");
        when(metaData.getPrimaryKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs4);
        when(rs4.getMetaData()).thenReturn(rsMetaData4);
        when(rsMetaData4.getColumnCount()).thenReturn(3);
        when(rsMetaData4.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData4.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs4.getString(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData4.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData4.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs4.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData4.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rsMetaData4.getColumnName(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rs4.getString(3)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rs4.next()).thenReturn(true).thenReturn(false);
        when(metaData.getIndexInfo(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean())).thenReturn(rs5);
        when(rs5.next()).thenReturn(true).thenReturn(false);
        when(rs5.getMetaData()).thenReturn(rsMetaData5);
        when(rsMetaData5.getColumnCount()).thenReturn(2);
        when(rsMetaData5.getColumnLabel(1)).thenReturn("INDEX_NAME");
        when(rsMetaData5.getColumnName(1)).thenReturn("INDEX_NAME");
        when(rs5.getString(1)).thenReturn("testIndexName");
        when(rsMetaData5.getColumnLabel(2)).thenReturn("TABLE_NAME");
        when(rsMetaData5.getColumnName(2)).thenReturn("TABLE_NAME");
        when(rs5.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(stmt3.executeQuery()).thenReturn(stmtrs3);
        when(stmtrs3.next()).thenReturn(true);
        when(stmt4.executeQuery()).thenReturn(stmtrs4);
        when(stmtrs4.next()).thenReturn(true);
        Table testTable = spyReader.readTable(DdlReaderTestConstants.CATALOG, DdlReaderTestConstants.SCHEMA, DdlReaderTestConstants.TABLE);
        Table expectedTable = new Table();
        expectedTable.setName(DdlReaderTestConstants.TESTNAME);
        expectedTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        expectedTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        expectedTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        expectedTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testColumn = new Column();
        testColumn.setDefaultValue(defaultValue);// Variable 1
        testColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testColumn.setJdbcTypeName(jdbcTypeName);// Variable 2
        testColumn.setSize(testColumnSize);
        testColumn.setAutoIncrement(true);
        testColumn.setJdbcTypeCode(jdbcTypeCode);// Variable 3
        testColumn.setMappedType(jdbcTypeName);// Variable 2
        testColumn.setPrecisionRadix(10);
        testColumn.setPrimaryKeySequence(1);
        testColumn.setPrimaryKey(true);
        PlatformColumn platformColumn = new PlatformColumn();
        platformColumn.setDecimalDigits(-1);
        platformColumn.setDefaultValue(defaultValue);// Variable 1
        platformColumn.setName("mssql2000");
        platformColumn.setType(jdbcTypeName);// Variable 2
        platformColumn.setSize(platformColumnSize);
        HashMap<String, PlatformColumn> expectedPlatformColumn = new HashMap<String, PlatformColumn>();
        expectedPlatformColumn.put("mssql2000", platformColumn);
        IndexColumn testIndexColumn = new IndexColumn();
        testIndexColumn.setOrdinalPosition(0);
        NonUniqueIndex testIndex = new NonUniqueIndex();
        testIndex.setName("testIndexName");
        testIndex.addColumn(testIndexColumn);
        testColumn.addPlatformColumn(platformColumn);
        expectedTable.addColumn(testColumn);
        expectedTable.addIndex(testIndex);
        assertEquals(expectedTable, testTable);
    }

    @ParameterizedTest
    @CsvSource({ "'1001001','1001001',BINARY," + Types.BINARY + ",100,BINARY," + Types.BINARY + ",BINARY," + 63 + ",BINARY",
            "testDef,testDefault,TEXT," + Types.VARCHAR + ",2147483647,TEXT," + Types.VARCHAR + ",LONGVARCHAR," + 2147483647 + ",VARCHAR",
            "testDef,testDefault,BINARY," + Types.BINARY + ",2147483647,BINARY," + Types.BINARY + ",BINARY," + 2147483647 + ",BINARY",
            "testDef,testDefault,OTHER," + Types.OTHER + ",1111,OTHER," + Types.OTHER + ",OTHER," + 63 + ",OTHER" })
    void testReadTableWithAdvancedArgs(String columnDef, String columnDefault, String jdbcTypeName, int jdbcTypeCode, String columnSize,
            String testColumnJdbcTypeName, int testColumnJdbcTypeCode, String testColumnMappedType, int platformColumnSize,
            String platformColumnType) throws Exception {
        // Mocked Components
        Connection connection = mock(Connection.class);
        DataSource dataSource = mock(DataSource.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);
        SqlTemplateSettings settings = mock(SqlTemplateSettings.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        // "Real" Components
        MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings, databaseInfo);
        MsSql2000DatabasePlatform platform = new MsSql2000DatabasePlatform(dataSource, settings);
        // Spied Components
        MsSql2000DatabasePlatform spyPlatform = Mockito.spy(platform);
        testTemplate.setIsolationLevel(1);
        MsSqlJdbcSqlTemplate spyTemplate = Mockito.spy(testTemplate);
        spyTemplate.setIsolationLevel(1);
        MsSqlDdlReader testReader = new MsSqlDdlReader(spyPlatform);
        MsSqlDdlReader spyReader = Mockito.spy(testReader);
        // Result Set Mocks
        ResultSet rs = mock(ResultSet.class);
        ResultSet rs2 = mock(ResultSet.class);
        ResultSet rs3 = mock(ResultSet.class);
        ResultSet rs4 = mock(ResultSet.class);
        ResultSet rs5 = mock(ResultSet.class);
        ResultSet stmtrs1 = mock(ResultSet.class);
        ResultSet stmtrs2 = mock(ResultSet.class);
        ResultSet stmtrs3 = mock(ResultSet.class);
        ResultSet stmtrs4 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData3 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData4 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData5 = mock(ResultSetMetaData.class);
        PreparedStatement stmt1 = mock(PreparedStatement.class);
        PreparedStatement stmt2 = mock(PreparedStatement.class);
        PreparedStatement stmt3 = mock(PreparedStatement.class);
        PreparedStatement stmt4 = mock(PreparedStatement.class);
        ResultSetMetaData stmt1RsMetaData = mock(ResultSetMetaData.class);
        when(spyTemplate.getDataSource().getConnection()).thenReturn(connection);
        doReturn(spyTemplate).when(spyPlatform).createSqlTemplate();
        when(spyPlatform.createSqlTemplate()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplateDirty()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplate()).thenReturn(spyTemplate);
        when(spyTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                (String[]) ArgumentMatchers.any())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(5);
        when(rsMetaData.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData.getColumnName(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rsMetaData.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rs.getString(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rsMetaData.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rs.getString(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        when(rsMetaData.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rsMetaData.getColumnName(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rs.getString(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        when(rsMetaData.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rsMetaData.getColumnName(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rs.getString(5)).thenReturn(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        when(metaData.getColumns(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(8);
        when(rsMetaData2.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rsMetaData2.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rs2.getString(1)).thenReturn(columnDef);
        when(rsMetaData2.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rsMetaData2.getColumnName(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rs2.getString(2)).thenReturn(columnDefault);
        when(rsMetaData2.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData2.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs2.getString(3)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData2.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData2.getColumnName(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs2.getString(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rsMetaData2.getColumnName(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rs2.getString(5)).thenReturn(jdbcTypeName);
        when(rsMetaData2.getColumnLabel(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rsMetaData2.getColumnName(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rs2.getInt(6)).thenReturn(jdbcTypeCode);
        when(rsMetaData2.getColumnLabel(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rsMetaData2.getColumnName(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rs2.getString(7)).thenReturn("TRUE");
        when(rsMetaData2.getColumnLabel(8)).thenReturn("COLUMN_SIZE");
        when(rsMetaData2.getColumnName(8)).thenReturn("COLUMN_SIZE");
        when(rs2.getString(8)).thenReturn(columnSize);
        when(metaData.getImportedKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs3);
        when(rs3.next()).thenReturn(false);
        when(rs3.getMetaData()).thenReturn(rsMetaData3);
        when(rsMetaData3.getColumnCount()).thenReturn(0);
        // when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3)
        // .thenReturn(stmt4);
        // THIS SECTION IS NEW. This is because the msSqlDdlReader uses the
        // determineAutoIncrementFromResultSetMetaData
        // method, and this changes several things when testing
        when(connection.createStatement()).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3).thenReturn(stmt4);
        when(stmt1.executeQuery(ArgumentMatchers.anyString())).thenReturn(stmtrs1);
        when(stmtrs1.getMetaData()).thenReturn(stmt1RsMetaData);
        when(stmt1RsMetaData.isAutoIncrement(ArgumentMatchers.anyInt())).thenReturn(true);
        when(stmtrs1.next()).thenReturn(true).thenReturn(false);
        when(stmtrs1.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAMECAPS);
        when(stmt2.executeQuery()).thenReturn(stmtrs2);
        when(stmtrs2.next()).thenReturn(true).thenReturn(false);
        when(stmtrs2.getString(1)).thenReturn("NOTRIGHTNAME");
        when(stmtrs2.getString(2)).thenReturn("TESTSCHEMA");
        when(metaData.getPrimaryKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs4);
        when(rs4.getMetaData()).thenReturn(rsMetaData4);
        when(rsMetaData4.getColumnCount()).thenReturn(3);
        when(rsMetaData4.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData4.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs4.getString(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData4.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData4.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs4.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData4.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rsMetaData4.getColumnName(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rs4.getString(3)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rs4.next()).thenReturn(true).thenReturn(false);
        when(metaData.getIndexInfo(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean())).thenReturn(rs5);
        when(rs5.next()).thenReturn(true).thenReturn(false);
        when(rs5.getMetaData()).thenReturn(rsMetaData5);
        when(rsMetaData5.getColumnCount()).thenReturn(2);
        when(rsMetaData5.getColumnLabel(1)).thenReturn("INDEX_NAME");
        when(rsMetaData5.getColumnName(1)).thenReturn("INDEX_NAME");
        when(rs5.getString(1)).thenReturn("testIndexName");
        when(rsMetaData5.getColumnLabel(2)).thenReturn("TABLE_NAME");
        when(rsMetaData5.getColumnName(2)).thenReturn("TABLE_NAME");
        when(rs5.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(stmt3.executeQuery()).thenReturn(stmtrs3);
        when(stmtrs3.next()).thenReturn(true);
        when(stmt4.executeQuery()).thenReturn(stmtrs4);
        when(stmtrs4.next()).thenReturn(true);
        doReturn(1).when(spyTemplate).queryForInt(ArgumentMatchers.anyString(), (Object) ArgumentMatchers.any());
        Table testTable = spyReader.readTable(DdlReaderTestConstants.CATALOG, DdlReaderTestConstants.SCHEMA, DdlReaderTestConstants.TABLE);
        Table expectedTable = new Table();
        expectedTable.setName(DdlReaderTestConstants.TESTNAME);
        expectedTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        expectedTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        expectedTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        expectedTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testColumn = new Column();
        testColumn.setDefaultValue(columnDef);
        testColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testColumn.setJdbcTypeName(testColumnJdbcTypeName);
        testColumn.setSize(columnSize);
        testColumn.setAutoIncrement(true);
        testColumn.setJdbcTypeCode(testColumnJdbcTypeCode);
        testColumn.setMappedType(testColumnMappedType);
        testColumn.setPrecisionRadix(10);
        testColumn.setPrimaryKeySequence(1);
        testColumn.setPrimaryKey(true);
        PlatformColumn platformColumn = new PlatformColumn();
        platformColumn.setDecimalDigits(-1);
        platformColumn.setDefaultValue(DdlReaderTestConstants.COLUMN_DEF_TEST_VALUE);
        platformColumn.setName("mssql2000");
        platformColumn.setSize(platformColumnSize);
        platformColumn.setType(platformColumnType);
        HashMap<String, PlatformColumn> expectedPlatformColumn = new HashMap<String, PlatformColumn>();
        expectedPlatformColumn.put("mssql2000", platformColumn);
        IndexColumn testIndexColumn = new IndexColumn();
        testIndexColumn.setOrdinalPosition(0);
        NonUniqueIndex testIndex = new NonUniqueIndex();
        testIndex.setName("testIndexName");
        testIndex.addColumn(testIndexColumn);
        testColumn.addPlatformColumn(platformColumn);
        expectedTable.addColumn(testColumn);
        expectedTable.addIndex(testIndex);
        assertEquals(expectedTable, testTable);
    }

    @ParameterizedTest
    // 1010 is a code size for null size and null scale. If scale is set, it
    // automatically sets the size to 0 instead of null, and for the test
    // purposes
    // we need the size to be null.
    // using 126:288 because it is supposed to set the size to 126,288 but the
    // comma breaks the csv data.
    @CsvSource({
            "1.1234321,1.1234321,DECIMAL," + Types.DECIMAL + ",0," + 288 + ",DECIMAL," + Types.DECIMAL + "," + Types.DECIMAL + ",0:288," + 288
                    + ",DECIMAL," + 2147483647 + ",DECIMAL",
            "1.1234321,1.1234321,DECIMAL," + Types.DECIMAL + ",126," + 288 + ",DECIMAL," + Types.DECIMAL + "," + Types.DECIMAL + ",126:288,"
                    + 288 + ",DECIMAL," + 63 + ",DECIMAL",
            "1.1234321,1.1234321,DECIMAL," + Types.DECIMAL + ",126," + 10 + ",DECIMAL," + Types.DECIMAL + "," + Types.DECIMAL + ",126:10,"
                    + 10 + ",DECIMAL," + 126 + ",DECIMAL" })
    void testReadTableWithDecimalArgs(String columnDef, String columnDefault, String jdbcTypeName, int jdbcTypeCode, String columnSize,
            int decimalDigits, String testColumnJdbcTypeName, int testColumnJdbcTypeCode, int testColumnMappedTypeCode, String testColumnSize,
            int testColumnScale, String testColumnMappedTypeName, int platformColumnSize, String platformColumnType) throws Exception {
        // Mocked Components
        Connection connection = mock(Connection.class);
        DataSource dataSource = mock(DataSource.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);
        SqlTemplateSettings settings = mock(SqlTemplateSettings.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        // "Real" Components
        MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings, databaseInfo);
        MsSql2000DatabasePlatform platform = new MsSql2000DatabasePlatform(dataSource, settings);
        // Spied Components
        MsSql2000DatabasePlatform spyPlatform = Mockito.spy(platform);
        testTemplate.setIsolationLevel(1);
        MsSqlJdbcSqlTemplate spyTemplate = Mockito.spy(testTemplate);
        spyTemplate.setIsolationLevel(1);
        MsSqlDdlReader testReader = new MsSqlDdlReader(spyPlatform);
        MsSqlDdlReader spyReader = Mockito.spy(testReader);
        // Result Set Mocks
        ResultSet rs = mock(ResultSet.class);
        ResultSet rs2 = mock(ResultSet.class);
        ResultSet rs3 = mock(ResultSet.class);
        ResultSet rs4 = mock(ResultSet.class);
        ResultSet rs5 = mock(ResultSet.class);
        ResultSet stmtrs1 = mock(ResultSet.class);
        ResultSet stmtrs2 = mock(ResultSet.class);
        ResultSet stmtrs3 = mock(ResultSet.class);
        ResultSet stmtrs4 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData3 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData4 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData5 = mock(ResultSetMetaData.class);
        PreparedStatement stmt1 = mock(PreparedStatement.class);
        PreparedStatement stmt2 = mock(PreparedStatement.class);
        PreparedStatement stmt3 = mock(PreparedStatement.class);
        PreparedStatement stmt4 = mock(PreparedStatement.class);
        ResultSetMetaData stmt1RsMetaData = mock(ResultSetMetaData.class);
        when(spyTemplate.getDataSource().getConnection()).thenReturn(connection);
        doReturn(spyTemplate).when(spyPlatform).createSqlTemplate();
        when(spyPlatform.createSqlTemplate()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplateDirty()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplate()).thenReturn(spyTemplate);
        when(spyTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                (String[]) ArgumentMatchers.any())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(5);
        when(rsMetaData.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData.getColumnName(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rsMetaData.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rs.getString(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rsMetaData.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rs.getString(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        when(rsMetaData.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rsMetaData.getColumnName(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rs.getString(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        when(rsMetaData.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rsMetaData.getColumnName(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rs.getString(5)).thenReturn(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        when(metaData.getColumns(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(9);
        when(rsMetaData2.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rsMetaData2.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rs2.getString(1)).thenReturn(columnDef);
        when(rsMetaData2.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rsMetaData2.getColumnName(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rs2.getString(2)).thenReturn(columnDefault);
        when(rsMetaData2.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData2.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs2.getString(3)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData2.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData2.getColumnName(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs2.getString(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rsMetaData2.getColumnName(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rs2.getString(5)).thenReturn(jdbcTypeName);
        when(rsMetaData2.getColumnLabel(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rsMetaData2.getColumnName(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rs2.getInt(6)).thenReturn(jdbcTypeCode);
        when(rsMetaData2.getColumnLabel(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rsMetaData2.getColumnName(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rs2.getString(7)).thenReturn("TRUE");
        when(rsMetaData2.getColumnLabel(8)).thenReturn("COLUMN_SIZE");
        when(rsMetaData2.getColumnName(8)).thenReturn("COLUMN_SIZE");
        when(rs2.getString(8)).thenReturn(columnSize);
        when(rsMetaData2.getColumnLabel(9)).thenReturn("DECIMAL_DIGITS");
        when(rsMetaData2.getColumnName(9)).thenReturn("DECIMAL_DIGITS");
        when(rs2.getInt(9)).thenReturn(decimalDigits);
        when(metaData.getImportedKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs3);
        when(rs3.next()).thenReturn(false);
        when(rs3.getMetaData()).thenReturn(rsMetaData3);
        when(rsMetaData3.getColumnCount()).thenReturn(0);
        // when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3)
        // .thenReturn(stmt4);
        // THIS SECTION IS NEW. This is because the msSqlDdlReader uses the
        // determineAutoIncrementFromResultSetMetaData
        // method, and this changes several things when testing
        when(connection.createStatement()).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3).thenReturn(stmt4);
        when(stmt1.executeQuery(ArgumentMatchers.anyString())).thenReturn(stmtrs1);
        when(stmtrs1.getMetaData()).thenReturn(stmt1RsMetaData);
        when(stmt1RsMetaData.isAutoIncrement(ArgumentMatchers.anyInt())).thenReturn(true);
        when(stmtrs1.next()).thenReturn(true).thenReturn(false);
        when(stmtrs1.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAMECAPS);
        when(stmt2.executeQuery()).thenReturn(stmtrs2);
        when(stmtrs2.next()).thenReturn(true).thenReturn(false);
        when(stmtrs2.getString(1)).thenReturn("NOTRIGHTNAME");
        when(stmtrs2.getString(2)).thenReturn("TESTSCHEMA");
        when(metaData.getPrimaryKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs4);
        when(rs4.getMetaData()).thenReturn(rsMetaData4);
        when(rsMetaData4.getColumnCount()).thenReturn(3);
        when(rsMetaData4.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData4.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs4.getString(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData4.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData4.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs4.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData4.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rsMetaData4.getColumnName(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rs4.getString(3)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rs4.next()).thenReturn(true).thenReturn(false);
        when(metaData.getIndexInfo(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean())).thenReturn(rs5);
        when(rs5.next()).thenReturn(true).thenReturn(false);
        when(rs5.getMetaData()).thenReturn(rsMetaData5);
        when(rsMetaData5.getColumnCount()).thenReturn(2);
        when(rsMetaData5.getColumnLabel(1)).thenReturn("INDEX_NAME");
        when(rsMetaData5.getColumnName(1)).thenReturn("INDEX_NAME");
        when(rs5.getString(1)).thenReturn("testIndexName");
        when(rsMetaData5.getColumnLabel(2)).thenReturn("TABLE_NAME");
        when(rsMetaData5.getColumnName(2)).thenReturn("TABLE_NAME");
        when(rs5.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(stmt3.executeQuery()).thenReturn(stmtrs3);
        when(stmtrs3.next()).thenReturn(true);
        when(stmt4.executeQuery()).thenReturn(stmtrs4);
        when(stmtrs4.next()).thenReturn(true);
        doReturn(1).when(spyTemplate).queryForInt(ArgumentMatchers.anyString(), (Object) ArgumentMatchers.any());
        Table testTable = spyReader.readTable(DdlReaderTestConstants.CATALOG, DdlReaderTestConstants.SCHEMA, DdlReaderTestConstants.TABLE);
        Table expectedTable = new Table();
        expectedTable.setName(DdlReaderTestConstants.TESTNAME);
        expectedTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        expectedTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        expectedTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        expectedTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testColumn = new Column();
        testColumn.setDefaultValue(columnDef);
        testColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testColumn.setJdbcTypeName(jdbcTypeName);
        if (testColumnSize.equals("1010")) {
            testColumn.setSize(null);
        } else if (testColumnSize.contains(":")) {
            String correctColumnSize = testColumnSize.replace(":", ",");
            testColumn.setSize(correctColumnSize);
            testColumn.setScale(testColumnScale);
        } else {
            testColumn.setSize(testColumnSize);
            testColumn.setScale(testColumnScale);
        }
        testColumn.setAutoIncrement(true);
        testColumn.setJdbcTypeCode(testColumnJdbcTypeCode);
        testColumn.setMappedTypeCode(testColumnMappedTypeCode);
        testColumn.setMappedType(testColumnMappedTypeName);
        testColumn.setPrecisionRadix(10);
        testColumn.setPrimaryKeySequence(1);
        testColumn.setPrimaryKey(true);
        PlatformColumn platformColumn = new PlatformColumn();
        platformColumn.setDecimalDigits(-1);
        platformColumn.setDefaultValue(columnDef);
        platformColumn.setName("mssql2000");
        platformColumn.setSize(-1);
        platformColumn.setType(platformColumnType);
        HashMap<String, PlatformColumn> expectedPlatformColumn = new HashMap<String, PlatformColumn>();
        expectedPlatformColumn.put("mssql2000", platformColumn);
        IndexColumn testIndexColumn = new IndexColumn();
        testIndexColumn.setOrdinalPosition(0);
        NonUniqueIndex testIndex = new NonUniqueIndex();
        testIndex.setName("testIndexName");
        testIndex.addColumn(testIndexColumn);
        testColumn.addPlatformColumn(platformColumn);
        expectedTable.addColumn(testColumn);
        expectedTable.addIndex(testIndex);
        assertEquals(expectedTable, testTable);
    }

    @Test
    void testReadTableWithBitType() throws Exception {
        // Mocked Components
        Connection connection = mock(Connection.class);
        DataSource dataSource = mock(DataSource.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);
        SqlTemplateSettings settings = mock(SqlTemplateSettings.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        // "Real" Components
        MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings, databaseInfo);
        MsSql2000DatabasePlatform platform = new MsSql2000DatabasePlatform(dataSource, settings);
        // Spied Components
        MsSql2000DatabasePlatform spyPlatform = Mockito.spy(platform);
        testTemplate.setIsolationLevel(1);
        MsSqlJdbcSqlTemplate spyTemplate = Mockito.spy(testTemplate);
        spyTemplate.setIsolationLevel(1);
        MsSqlDdlReader testReader = new MsSqlDdlReader(spyPlatform);
        MsSqlDdlReader spyReader = Mockito.spy(testReader);
        // Result Set Mocks
        ResultSet rs = mock(ResultSet.class);
        ResultSet rs2 = mock(ResultSet.class);
        ResultSet rs3 = mock(ResultSet.class);
        ResultSet rs4 = mock(ResultSet.class);
        ResultSet rs5 = mock(ResultSet.class);
        ResultSet stmtrs1 = mock(ResultSet.class);
        ResultSet stmtrs2 = mock(ResultSet.class);
        ResultSet stmtrs3 = mock(ResultSet.class);
        ResultSet stmtrs4 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData3 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData4 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData5 = mock(ResultSetMetaData.class);
        PreparedStatement stmt1 = mock(PreparedStatement.class);
        PreparedStatement stmt2 = mock(PreparedStatement.class);
        PreparedStatement stmt3 = mock(PreparedStatement.class);
        PreparedStatement stmt4 = mock(PreparedStatement.class);
        ResultSetMetaData stmt1RsMetaData = mock(ResultSetMetaData.class);
        when(spyTemplate.getDataSource().getConnection()).thenReturn(connection);
        doReturn(spyTemplate).when(spyPlatform).createSqlTemplate();
        when(spyPlatform.createSqlTemplate()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplateDirty()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplate()).thenReturn(spyTemplate);
        when(spyTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                (String[]) ArgumentMatchers.any())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(5);
        when(rsMetaData.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData.getColumnName(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rsMetaData.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rs.getString(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rsMetaData.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rs.getString(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        when(rsMetaData.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rsMetaData.getColumnName(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rs.getString(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        when(rsMetaData.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rsMetaData.getColumnName(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rs.getString(5)).thenReturn(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        when(metaData.getColumns(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(8);
        when(rsMetaData2.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rsMetaData2.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rs2.getString(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rsMetaData2.getColumnName(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rs2.getString(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData2.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs2.getString(3)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData2.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData2.getColumnName(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs2.getString(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rsMetaData2.getColumnName(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rs2.getString(5)).thenReturn("BIT");
        when(rsMetaData2.getColumnLabel(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rsMetaData2.getColumnName(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rs2.getInt(6)).thenReturn(Types.BIT);
        when(rsMetaData2.getColumnLabel(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rsMetaData2.getColumnName(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rs2.getString(7)).thenReturn("TRUE");
        when(rsMetaData2.getColumnLabel(8)).thenReturn("COLUMN_SIZE");
        when(rsMetaData2.getColumnName(8)).thenReturn("COLUMN_SIZE");
        when(rs2.getString(8)).thenReturn("63");
        when(metaData.getImportedKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs3);
        when(rs3.next()).thenReturn(false);
        when(rs3.getMetaData()).thenReturn(rsMetaData3);
        when(rsMetaData3.getColumnCount()).thenReturn(0);
        //
        when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3)
                .thenReturn(stmt4);
        // THIS SECTION IS NEW. This is because the msSqlDdlReader uses the
        // determineAutoIncrementFromResultSetMetaData
        // method, and this changes several things when testing
        when(connection.createStatement()).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3).thenReturn(stmt4);
        when(stmt1.executeQuery(ArgumentMatchers.anyString())).thenReturn(stmtrs1);
        when(stmtrs1.getMetaData()).thenReturn(stmt1RsMetaData);
        when(stmt1RsMetaData.isAutoIncrement(ArgumentMatchers.anyInt())).thenReturn(true);
        when(stmtrs1.next()).thenReturn(true).thenReturn(false);
        when(stmtrs1.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAMECAPS);
        when(stmt2.executeQuery()).thenReturn(stmtrs2);
        when(stmtrs2.next()).thenReturn(true).thenReturn(false);
        when(stmtrs2.getString(1)).thenReturn("NOTRIGHTNAME");
        when(stmtrs2.getString(2)).thenReturn("TESTSCHEMA");
        when(metaData.getPrimaryKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs4);
        when(rs4.getMetaData()).thenReturn(rsMetaData4);
        when(rsMetaData4.getColumnCount()).thenReturn(3);
        when(rsMetaData4.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData4.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs4.getString(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData4.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData4.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs4.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData4.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rsMetaData4.getColumnName(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rs4.getString(3)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rs4.next()).thenReturn(true).thenReturn(false);
        when(metaData.getIndexInfo(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean())).thenReturn(rs5);
        when(rs5.next()).thenReturn(true).thenReturn(false);
        when(rs5.getMetaData()).thenReturn(rsMetaData5);
        when(rsMetaData5.getColumnCount()).thenReturn(2);
        when(rsMetaData5.getColumnLabel(1)).thenReturn("INDEX_NAME");
        when(rsMetaData5.getColumnName(1)).thenReturn("INDEX_NAME");
        when(rs5.getString(1)).thenReturn("testIndexName");
        when(rsMetaData5.getColumnLabel(2)).thenReturn("TABLE_NAME");
        when(rsMetaData5.getColumnName(2)).thenReturn("TABLE_NAME");
        when(rs5.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(stmt3.executeQuery()).thenReturn(stmtrs3);
        when(stmtrs3.next()).thenReturn(true);
        when(stmt4.executeQuery()).thenReturn(stmtrs4);
        when(stmtrs4.next()).thenReturn(true);
        Table testTable = spyReader.readTable(DdlReaderTestConstants.CATALOG, DdlReaderTestConstants.SCHEMA, DdlReaderTestConstants.TABLE);
        Table expectedTable = new Table();
        expectedTable.setName(DdlReaderTestConstants.TESTNAME);
        expectedTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        expectedTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        expectedTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        expectedTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testColumn = new Column();
        testColumn.setDefaultValue(DdlReaderTestConstants.COLUMN_DEF_TEST_VALUE);
        testColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testColumn.setJdbcTypeName("BIT");
        testColumn.setSize("63");
        testColumn.setAutoIncrement(true);
        testColumn.setJdbcTypeCode(Types.BIT);
        testColumn.setMappedType("BIT");
        testColumn.setPrecisionRadix(10);
        testColumn.setPrimaryKeySequence(1);
        testColumn.setPrimaryKey(true);
        PlatformColumn platformColumn = new PlatformColumn();
        platformColumn.setDecimalDigits(-1);
        platformColumn.setDefaultValue(DdlReaderTestConstants.COLUMN_DEF_TEST_VALUE);
        platformColumn.setName("mssql2000");
        platformColumn.setSize(63);
        platformColumn.setType("VARCHAR");
        HashMap<String, PlatformColumn> expectedPlatformColumn = new HashMap<String, PlatformColumn>();
        expectedPlatformColumn.put("mssql2000", platformColumn);
        IndexColumn testIndexColumn = new IndexColumn();
        testIndexColumn.setOrdinalPosition(0);
        NonUniqueIndex testIndex = new NonUniqueIndex();
        testIndex.setName("testIndexName");
        testIndex.addColumn(testIndexColumn);
        testColumn.addPlatformColumn(platformColumn);
        expectedTable.addColumn(testColumn);
        expectedTable.addIndex(testIndex);
        assertEquals(expectedTable, testTable);
    }

    @ParameterizedTest
    // 1010 is a code size for null size and null scale. If scale is set, it
    // automatically sets the size to 0 instead of null, and for the test
    // purposes
    // we need the size to be null.
    // using 126:288 because it is supposed to set the size to 126,288 but the
    // comma breaks the csv data.
    @CsvSource({
            "1.1234321,1.1234321,GEOMETRY," + Types.VARCHAR + ",0," + 288 + ",VARCHAR," + Types.VARCHAR + "," + Types.VARCHAR + ",0," + 288
                    + ",VARCHAR," + 2147483647 + ",VARCHAR",
            "1.1234321,1.1234321,GEOGRAPHY," + Types.VARCHAR + ",126," + 288 + ",VARCHAR," + Types.VARCHAR + "," + Types.VARCHAR + ",126:288,"
                    + 288 + ",VARCHAR," + 63 + ",VARCHAR" })
    void testReadTableWithMsSQLSpecificArgs(String columnDef, String columnDefault, String jdbcTypeName, int jdbcTypeCode, String columnSize,
            int decimalDigits, String testColumnJdbcTypeName, int testColumnJdbcTypeCode, int testColumnMappedTypeCode, String testColumnSize,
            int testColumnScale, String testColumnMappedTypeName, int platformColumnSize, String platformColumnType) throws Exception {
        // Mocked Components
        Connection connection = mock(Connection.class);
        DataSource dataSource = mock(DataSource.class);
        DatabaseInfo databaseInfo = mock(DatabaseInfo.class);
        SqlTemplateSettings settings = mock(SqlTemplateSettings.class);
        DatabaseMetaData metaData = mock(DatabaseMetaData.class);
        // "Real" Components
        MsSqlJdbcSqlTemplate testTemplate = new MsSqlJdbcSqlTemplate(dataSource, settings, databaseInfo);
        MsSql2000DatabasePlatform platform = new MsSql2000DatabasePlatform(dataSource, settings);
        // Spied Components
        MsSql2000DatabasePlatform spyPlatform = Mockito.spy(platform);
        testTemplate.setIsolationLevel(1);
        MsSqlJdbcSqlTemplate spyTemplate = Mockito.spy(testTemplate);
        spyTemplate.setIsolationLevel(1);
        MsSqlDdlReader testReader = new MsSqlDdlReader(spyPlatform);
        MsSqlDdlReader spyReader = Mockito.spy(testReader);
        // Result Set Mocks
        ResultSet rs = mock(ResultSet.class);
        ResultSet rs2 = mock(ResultSet.class);
        ResultSet rs3 = mock(ResultSet.class);
        ResultSet rs4 = mock(ResultSet.class);
        ResultSet rs5 = mock(ResultSet.class);
        ResultSet stmtrs1 = mock(ResultSet.class);
        ResultSet stmtrs2 = mock(ResultSet.class);
        ResultSet stmtrs3 = mock(ResultSet.class);
        ResultSet stmtrs4 = mock(ResultSet.class);
        ResultSetMetaData rsMetaData = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData2 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData3 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData4 = mock(ResultSetMetaData.class);
        ResultSetMetaData rsMetaData5 = mock(ResultSetMetaData.class);
        PreparedStatement stmt1 = mock(PreparedStatement.class);
        PreparedStatement stmt2 = mock(PreparedStatement.class);
        PreparedStatement stmt3 = mock(PreparedStatement.class);
        PreparedStatement stmt4 = mock(PreparedStatement.class);
        ResultSetMetaData stmt1RsMetaData = mock(ResultSetMetaData.class);
        when(spyTemplate.getDataSource().getConnection()).thenReturn(connection);
        doReturn(spyTemplate).when(spyPlatform).createSqlTemplate();
        when(spyPlatform.createSqlTemplate()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplateDirty()).thenReturn(spyTemplate);
        when(spyPlatform.getSqlTemplate()).thenReturn(spyTemplate);
        when(spyTemplate.getDataSource()).thenReturn(dataSource);
        when(dataSource.getConnection()).thenReturn(connection);
        when(connection.getMetaData()).thenReturn(metaData);
        when(metaData.getTables(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                (String[]) ArgumentMatchers.any())).thenReturn(rs);
        when(rs.next()).thenReturn(true).thenReturn(false);
        when(rs.getMetaData()).thenReturn(rsMetaData);
        when(rsMetaData.getColumnCount()).thenReturn(5);
        when(rsMetaData.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData.getColumnName(1)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rsMetaData.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE);
        when(rs.getString(2)).thenReturn(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        when(rsMetaData.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rsMetaData.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT);
        when(rs.getString(3)).thenReturn(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        when(rsMetaData.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rsMetaData.getColumnName(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEM);
        when(rs.getString(4)).thenReturn(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        when(rsMetaData.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rsMetaData.getColumnName(5)).thenReturn(DdlReaderTestConstants.REMARKS);
        when(rs.getString(5)).thenReturn(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        when(metaData.getColumns(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.any())).thenReturn(rs2);
        when(rs2.next()).thenReturn(true).thenReturn(false);
        when(rs2.getMetaData()).thenReturn(rsMetaData2);
        when(rsMetaData2.getColumnCount()).thenReturn(9);
        when(rsMetaData2.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rsMetaData2.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_DEF);
        when(rs2.getString(1)).thenReturn(columnDef);
        when(rsMetaData2.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rsMetaData2.getColumnName(2)).thenReturn(DdlReaderTestConstants.COLUMN_DEFAULT);
        when(rs2.getString(2)).thenReturn(columnDefault);
        when(rsMetaData2.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData2.getColumnName(3)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs2.getString(3)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData2.getColumnLabel(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData2.getColumnName(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs2.getString(4)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData2.getColumnLabel(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rsMetaData2.getColumnName(5)).thenReturn(DdlReaderTestConstants.TYPE_NAME);
        when(rs2.getString(5)).thenReturn(jdbcTypeName);
        when(rsMetaData2.getColumnLabel(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rsMetaData2.getColumnName(6)).thenReturn(DdlReaderTestConstants.DATA_TYPE);
        when(rs2.getInt(6)).thenReturn(jdbcTypeCode);
        when(rsMetaData2.getColumnLabel(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rsMetaData2.getColumnName(7)).thenReturn(DdlReaderTestConstants.IS_NULLABLE);
        when(rs2.getString(7)).thenReturn("TRUE");
        when(rsMetaData2.getColumnLabel(8)).thenReturn("COLUMN_SIZE");
        when(rsMetaData2.getColumnName(8)).thenReturn("COLUMN_SIZE");
        when(rs2.getString(8)).thenReturn(columnSize);
        when(rsMetaData2.getColumnLabel(9)).thenReturn("DECIMAL_DIGITS");
        when(rsMetaData2.getColumnName(9)).thenReturn("DECIMAL_DIGITS");
        when(rs2.getInt(9)).thenReturn(decimalDigits);
        when(metaData.getImportedKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs3);
        when(rs3.next()).thenReturn(false);
        when(rs3.getMetaData()).thenReturn(rsMetaData3);
        when(rsMetaData3.getColumnCount()).thenReturn(0);
        // when(connection.prepareStatement(ArgumentMatchers.anyString())).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3)
        // .thenReturn(stmt4);
        // THIS SECTION IS NEW. This is because the msSqlDdlReader uses the
        // determineAutoIncrementFromResultSetMetaData
        // method, and this changes several things when testing
        when(connection.createStatement()).thenReturn(stmt1).thenReturn(stmt2).thenReturn(stmt3).thenReturn(stmt4);
        when(stmt1.executeQuery(ArgumentMatchers.anyString())).thenReturn(stmtrs1);
        when(stmtrs1.getMetaData()).thenReturn(stmt1RsMetaData);
        when(stmt1RsMetaData.isAutoIncrement(ArgumentMatchers.anyInt())).thenReturn(true);
        when(stmtrs1.next()).thenReturn(true).thenReturn(false);
        when(stmtrs1.getString(1)).thenReturn(DdlReaderTestConstants.TESTNAMECAPS);
        when(stmt2.executeQuery()).thenReturn(stmtrs2);
        when(stmtrs2.next()).thenReturn(true).thenReturn(false);
        when(stmtrs2.getString(1)).thenReturn("NOTRIGHTNAME");
        when(stmtrs2.getString(2)).thenReturn("TESTSCHEMA");
        when(metaData.getPrimaryKeys(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString()))
                .thenReturn(rs4);
        when(rs4.getMetaData()).thenReturn(rsMetaData4);
        when(rsMetaData4.getColumnCount()).thenReturn(3);
        when(rsMetaData4.getColumnLabel(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rsMetaData4.getColumnName(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME);
        when(rs4.getString(1)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rsMetaData4.getColumnLabel(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rsMetaData4.getColumnName(2)).thenReturn(DdlReaderTestConstants.TABLE_NAME);
        when(rs4.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(rsMetaData4.getColumnLabel(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rsMetaData4.getColumnName(3)).thenReturn(DdlReaderTestConstants.PK_NAME);
        when(rs4.getString(3)).thenReturn(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        when(rs4.next()).thenReturn(true).thenReturn(false);
        when(metaData.getIndexInfo(ArgumentMatchers.anyString(), ArgumentMatchers.anyString(), ArgumentMatchers.anyString(),
                ArgumentMatchers.anyBoolean(), ArgumentMatchers.anyBoolean())).thenReturn(rs5);
        when(rs5.next()).thenReturn(true).thenReturn(false);
        when(rs5.getMetaData()).thenReturn(rsMetaData5);
        when(rsMetaData5.getColumnCount()).thenReturn(2);
        when(rsMetaData5.getColumnLabel(1)).thenReturn("INDEX_NAME");
        when(rsMetaData5.getColumnName(1)).thenReturn("INDEX_NAME");
        when(rs5.getString(1)).thenReturn("testIndexName");
        when(rsMetaData5.getColumnLabel(2)).thenReturn("TABLE_NAME");
        when(rsMetaData5.getColumnName(2)).thenReturn("TABLE_NAME");
        when(rs5.getString(2)).thenReturn(DdlReaderTestConstants.TESTNAME);
        when(stmt3.executeQuery()).thenReturn(stmtrs3);
        when(stmtrs3.next()).thenReturn(true);
        when(stmt4.executeQuery()).thenReturn(stmtrs4);
        when(stmtrs4.next()).thenReturn(true);
        doReturn(1).when(spyTemplate).queryForInt(ArgumentMatchers.anyString(), (Object) ArgumentMatchers.any());
        Table testTable = spyReader.readTable(DdlReaderTestConstants.CATALOG, DdlReaderTestConstants.SCHEMA, DdlReaderTestConstants.TABLE);
        Table expectedTable = new Table();
        expectedTable.setName(DdlReaderTestConstants.TESTNAME);
        expectedTable.setType(DdlReaderTestConstants.TABLE_TYPE_TEST_VALUE);
        expectedTable.setCatalog(DdlReaderTestConstants.TABLE_CAT_TEST_VALUE);
        expectedTable.setSchema(DdlReaderTestConstants.TABLE_SCHEMA_TEST_VALUE);
        expectedTable.setDescription(DdlReaderTestConstants.REMARKS_TEST_VALUE);
        Column testColumn = new Column();
        testColumn.setDefaultValue(columnDef);
        testColumn.setName(DdlReaderTestConstants.COLUMN_NAME_TEST_VALUE);
        testColumn.setJdbcTypeName(jdbcTypeName);
        if (testColumnSize.equals("1010")) {
            testColumn.setSize(null);
        } else if (testColumnSize.contains(":")) {
            String correctColumnSize = testColumnSize.replace(":", ",");
            testColumn.setSize(correctColumnSize);
            testColumn.setScale(testColumnScale);
        } else {
            testColumn.setSize(testColumnSize);
            testColumn.setScale(testColumnScale);
        }
        testColumn.setAutoIncrement(true);
        testColumn.setJdbcTypeCode(testColumnJdbcTypeCode);
        testColumn.setMappedTypeCode(testColumnMappedTypeCode);
        testColumn.setMappedType(testColumnMappedTypeName);
        testColumn.setPrecisionRadix(10);
        testColumn.setPrimaryKeySequence(1);
        testColumn.setPrimaryKey(true);
        PlatformColumn platformColumn = new PlatformColumn();
        platformColumn.setDecimalDigits(-1);
        platformColumn.setDefaultValue(columnDef);
        platformColumn.setName("mssql2000");
        platformColumn.setSize(-1);
        platformColumn.setType(platformColumnType);
        HashMap<String, PlatformColumn> expectedPlatformColumn = new HashMap<String, PlatformColumn>();
        expectedPlatformColumn.put("mssql2000", platformColumn);
        IndexColumn testIndexColumn = new IndexColumn();
        testIndexColumn.setOrdinalPosition(0);
        NonUniqueIndex testIndex = new NonUniqueIndex();
        testIndex.setName("testIndexName");
        testIndex.addColumn(testIndexColumn);
        testColumn.addPlatformColumn(platformColumn);
        expectedTable.addColumn(testColumn);
        expectedTable.addIndex(testIndex);
        assertEquals(expectedTable, testTable);
    }

    protected String getResultSetSchemaName() {
        return DdlReaderTestConstants.TABLE_SCHEM;
    }

    public IDatabasePlatform getPlatform() {
        return platform;
    }
}