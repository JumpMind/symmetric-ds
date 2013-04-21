package org.jumpmind.symmetric;

import java.io.File;
import java.sql.Types;
import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.ObjectUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.symmetric.io.data.DbExport;
import org.jumpmind.symmetric.io.data.DbExport.Compatible;
import org.jumpmind.symmetric.io.data.DbExport.Format;
import org.jumpmind.symmetric.io.data.DbFill;
import org.jumpmind.symmetric.io.data.DbImport;
import org.jumpmind.symmetric.io.data.writer.ConflictException;
import org.jumpmind.symmetric.service.impl.AbstractServiceTest;
import org.junit.BeforeClass;
import org.junit.Test;

public class DbExportImportTest extends AbstractServiceTest {

    private static final String SELECT_FROM_TEST_DB_IMPORT_1_ORDER_BY_ID = "select * from test_db_import_1 order by id";

    private static final String TEST_TS_W_TZ = "test_ts_w_tz";
    
    protected static IDatabasePlatform platform;

    @BeforeClass
    public static void setup()  throws Exception {
        File f = new File("target/rootdbs");
        FileUtils.deleteDirectory(f);
        f.mkdir();
        AbstractServiceTest.setup();

    }
    
    @Test
    public void testInsertBigIntIntoOracleIntField() {
        if (getPlatform().getName().equals(DatabaseNamesConstants.ORACLE)) {
            ISymmetricEngine engine = getSymmetricEngine();
            IDatabasePlatform platform = engine.getDatabasePlatform();
            
            Table table = new Table("TEST_ORACLE_INTEGER");
            table.addColumn(new Column("A", false, Types.INTEGER, -1, -1));
            platform.alterCaseToMatchDatabaseDefaultCase(table);
            platform.createTables(true, false, table);
            
            DbImport importer = new DbImport(platform);
            importer.setFormat(DbImport.Format.CSV);
            importer.importTables("\"A\"\n1149140000100490", table.getName());
            
            Assert.assertEquals(1149140000100490l,platform.getSqlTemplate().queryForLong("select A from TEST_ORACLE_INTEGER"));
        }
    }

    @Test 
    public void exportNullTimestampToCsv() throws Exception {       
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getDatabasePlatform();
        
        Table table = new Table("test_null_timestamp");
        table.addColumn(new Column("a", false, Types.TIMESTAMP, -1, -1));
        table.addColumn(new Column("b", false, Types.TIMESTAMP, -1, -1));
        platform.alterCaseToMatchDatabaseDefaultCase(table);
        platform.createTables(true, false, table);
        
        platform.getSqlTemplate().update("insert into test_null_timestamp values(null, null)");
        
        DbExport export = new DbExport(platform);
        export.setNoCreateInfo(true);
        export.setFormat(Format.CSV);
        
        String csv = export.exportTables(new Table[] {table});
        
        Assert.assertEquals("\"A\",\"B\"\n,", csv.trim().toUpperCase());
        
    }
    
    @Test
    public void exportTableInAnotherSchemaOnH2() throws Exception {
        if (getPlatform().getName().equals(DatabaseNamesConstants.H2)) {
            ISymmetricEngine engine = getSymmetricEngine();
            ISqlTemplate template = getPlatform().getSqlTemplate();
            template.update("CREATE SCHEMA IF NOT EXISTS A");
            template.update("CREATE TABLE IF NOT EXISTS A.TEST (ID INT, NOTES VARCHAR(100), PRIMARY KEY (ID))");
            template.update("DELETE FROM A.TEST");
            template.update("INSERT INTO A.TEST VALUES(1,'test')");

            DbExport export = new DbExport(engine.getDatabasePlatform());
            export.setSchema("A");
            export.setFormat(Format.SQL);
            export.setNoCreateInfo(false);
            export.setNoData(false);

            export.exportTables(new String[] { "TEST" }).toLowerCase();
            // TODO validate
        }
    }   

    @Test
    public void exportTestDatabaseSQL() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        Table[] tables = engine.getSymmetricDialect().readSymmetricSchemaFromXml().getTables();

        DbExport export = new DbExport(engine.getDatabasePlatform());
        export.setFormat(Format.SQL);
        export.setNoCreateInfo(false);
        export.setNoData(true);
        export.setSchema(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultSchema());
        export.setCatalog(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultCatalog());
        export.setCompatible(Compatible.H2);
        String output = export.exportTables(tables).toLowerCase();

        Assert.assertEquals(output, 39, StringUtils.countMatches(output, "create table \"sym_"));
        Assert.assertEquals(35,
                StringUtils.countMatches(output, "varchar(" + Integer.MAX_VALUE + ")"));
    }

    @Test
    public void exportThenImportXml() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database testTables = platform.readDatabaseFromXml("/test-dbimport.xml", true);
        platform.dropDatabase(testTables, true);
        Table table = testTables.findTable("test_db_import_1", false);

        DbImport dbImport = new DbImport(platform);
        dbImport.setFormat(DbImport.Format.XML);
        dbImport.setSchema(platform.getDefaultSchema());
        dbImport.setCatalog(platform.getDefaultCatalog());
        dbImport.setAlterCaseToMatchDatabaseDefaultCase(true);
        dbImport.importTables(getClass().getResourceAsStream("/test-dbimport.xml"));

        DbExport export = new DbExport(platform);
        export.setFormat(Format.XML);
        export.setNoCreateInfo(false);
        export.setNoData(true);
        export.setSchema(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultSchema());
        export.setCatalog(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultCatalog());
        export.exportTables(new String[] { table.getName() });

        // System.out.println(output);
        // TODO validate

    }

    @Test
    public void testExportTimestampWithTimeZone() throws Exception {
        if (createAndFillTimestampWithTimeZoneTable()) {
            ISymmetricEngine engine = getSymmetricEngine();

            DbExport export = new DbExport(engine.getDatabasePlatform());
            export.setCompatible(Compatible.POSTGRES);
            export.setFormat(Format.SQL);
            String sql = export.exportTables(new String[] { TEST_TS_W_TZ });
            final String EXPECTED_POSTGRES = "insert into \"test_ts_w_tz\"(\"id\", \"tz\") (select 1,cast('1973-06-08 07:00:00.000000 -04:00' as timestamp with time zone) where (select distinct 1 from \"test_ts_w_tz\" where  \"id\" = 1) is null);";
            Assert.assertTrue("Expected the following sql:\n" +sql + "\n\n to contain:\n" +EXPECTED_POSTGRES, sql.contains(EXPECTED_POSTGRES));
            
            export.setCompatible(Compatible.ORACLE);
            sql = export.exportTables(new String[] { TEST_TS_W_TZ });
            final String EXPECTED_ORACLE = "insert into \"test_ts_w_tz\" (\"id\", \"tz\") values (1,TO_TIMESTAMP_TZ('1973-06-08 07:00:00.000000 -04:00', 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM'));";
            Assert.assertTrue("Expected the following sql:\n" +sql + "\n\n to contain:\n" +EXPECTED_ORACLE, sql.contains(EXPECTED_ORACLE));

        }
    }

    protected boolean createAndFillTimestampWithTimeZoneTable() {
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getDatabasePlatform();
        String dbName = platform.getName();
        if (dbName.equals(DatabaseNamesConstants.ORACLE)
                || dbName.equals(DatabaseNamesConstants.POSTGRESQL)) {
            ISqlTemplate template = engine.getSqlTemplate();
            try {
                template.update(String.format("drop table \"%s\"", TEST_TS_W_TZ));
            } catch (Exception ex) {
            }
            String createSql = String.format(
                    "create table \"%s\" (\"id\" integer, \"tz\" timestamp with time zone, primary key (\"id\"))",
                    TEST_TS_W_TZ);
            template.update(createSql);          
            DmlStatement statement = platform.createDmlStatement(DmlType.INSERT, platform.getTableFromCache(TEST_TS_W_TZ, true));            
            template.update(statement.getSql(), statement.getValueArray(new Object[] {1, "1973-06-08 07:00:00.000 -04:00"}, new Object[] {1}));
            return true;
        } else {
            return false;
        }

    }

    protected void recreateImportTable() {
        ISymmetricEngine engine = getSymmetricEngine();
        DbImport reCreateTablesImport = new DbImport(engine.getDatabasePlatform());
        reCreateTablesImport.setFormat(DbImport.Format.XML);
        reCreateTablesImport.setDropIfExists(true);
        reCreateTablesImport.setAlterCaseToMatchDatabaseDefaultCase(true);
        reCreateTablesImport.importTables(getClass().getResourceAsStream("/test-dbimport.xml"));
    }

    protected void assertCountDbImportTableRecords(int expected) {
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database testTables = platform.readDatabaseFromXml("/test-dbimport.xml", true);
        Table table = testTables.findTable("test_db_import_1", false);
        DmlStatement dml = new DmlStatement(DmlType.COUNT, table.getCatalog(), table.getSchema(),
                table.getName(), null, table.getColumns(), false, null, null);
        Assert.assertEquals(expected, platform.getSqlTemplate().queryForInt(dml.getSql()));
    }

    @Test
    public void importSqlData() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();

        recreateImportTable();

        assertCountDbImportTableRecords(0);

        DbImport importCsv = new DbImport(engine.getDatabasePlatform());
        importCsv.setFormat(DbImport.Format.SQL);
        importCsv.importTables(getClass().getResourceAsStream("/test-dbimport-1-good.sql"));

        assertCountDbImportTableRecords(5);

        recreateImportTable();

        assertCountDbImportTableRecords(0);

        try {
            importCsv.importTables(getClass()
                    .getResourceAsStream("/test-dbimport-1-bad-line-2.sql"));
            Assert.fail("Expected a sql exception");
        } catch (SqlException ex) {
        }

        assertCountDbImportTableRecords(0);

        importCsv.setCommitRate(1);
        importCsv.setForceImport(true);
        importCsv.importTables(getClass().getResourceAsStream("/test-dbimport-1-bad-line-2.sql"));
        assertCountDbImportTableRecords(4);

    }

    @Test
    public void importSymXmlData() throws Exception {
        final String FILE = "/test-dbimport-1-sym_xml-1.xml";
        ISymmetricEngine engine = getSymmetricEngine();

        recreateImportTable();

        assertCountDbImportTableRecords(0);

        DbImport importCsv = new DbImport(engine.getDatabasePlatform());
        importCsv.setFormat(DbImport.Format.SYM_XML);
        importCsv.importTables(getClass().getResourceAsStream(FILE));

        assertCountDbImportTableRecords(2);

        try {
            importCsv.importTables(getClass().getResourceAsStream(FILE));
            Assert.fail("Expected a sql exception");
        } catch (ConflictException ex) {
        }

        assertCountDbImportTableRecords(2);

        recreateImportTable();

        importCsv.setReplaceRows(true);
        importCsv.importTables(getClass().getResourceAsStream(FILE));

        assertCountDbImportTableRecords(2);

    }

    @Test
    public void importXmlData() throws Exception {
        final String FILE = "/test-dbimport-1-xml-1.xml";
        ISymmetricEngine engine = getSymmetricEngine();

        DbImport importer = new DbImport(engine.getDatabasePlatform());
        importer.setFormat(DbImport.Format.XML);
        importer.setDropIfExists(true);
        importer.setAlterCaseToMatchDatabaseDefaultCase(true);
        importer.importTables(getClass().getResourceAsStream(FILE));

        assertCountDbImportTableRecords(3);

        // table should be dropped so this should work again
        importer.importTables(getClass().getResourceAsStream(FILE));

        assertCountDbImportTableRecords(3);

    }

    @Test
    public void exportThenImportCsv() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database testTables = platform.readDatabaseFromXml("/test-dbimport.xml", true);
        Table table = testTables.findTable("test_db_import_1", false);

        recreateImportTable();

        final int RECORD_COUNT = 100;

        DbFill fill = new DbFill(platform);
        fill.setRecordCount(RECORD_COUNT);
        fill.fillTables(table.getName());

        DbExport export = new DbExport(platform);
        export.setFormat(Format.CSV);
        export.setNoCreateInfo(true);
        export.setNoData(false);
        String csvOutput = export.exportTables(new String[] { table.getName() });

        ISqlTemplate sqlTemplate = platform.getSqlTemplate();

        List<Row> rowsBeforeImport = sqlTemplate.query(SELECT_FROM_TEST_DB_IMPORT_1_ORDER_BY_ID);

        recreateImportTable();

        DbImport importCsv = new DbImport(platform);
        importCsv.setFormat(DbImport.Format.CSV);
        importCsv.importTables(csvOutput, table.getName());

        DmlStatement dml = new DmlStatement(DmlType.COUNT, table.getCatalog(), table.getSchema(),
                table.getName(), null, table.getColumns(), false, null, null);
        Assert.assertEquals(RECORD_COUNT, sqlTemplate.queryForInt(dml.getSql()));

        compareRows(rowsBeforeImport, sqlTemplate.query(SELECT_FROM_TEST_DB_IMPORT_1_ORDER_BY_ID));

        // TODO test error

        // TODO test replace

        // TODO test ignore

        // TODO test force
    }
    
    @Test
    public void exportThenImportCsvWithBackslashes() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database testTables = platform.readDatabaseFromXml("/test-dbimport.xml", true);
        Table table = testTables.findTable("test_db_import_1", false);

        recreateImportTable();
        
        DbImport importCsv = new DbImport(platform);
        importCsv.setFormat(DbImport.Format.SQL);
        importCsv.importTables(getClass().getResourceAsStream("/test-dbimport-1-backslashes.sql"));

        assertCountDbImportTableRecords(1);

        DbExport export = new DbExport(platform);
        export.setFormat(Format.CSV);
        export.setNoCreateInfo(true);
        export.setNoData(false);
        String csvOutput = export.exportTables(new String[] { table.getName() });

        ISqlTemplate sqlTemplate = platform.getSqlTemplate();

        List<Row> rowsBeforeImport = sqlTemplate.query(SELECT_FROM_TEST_DB_IMPORT_1_ORDER_BY_ID);

        recreateImportTable();

        importCsv.setFormat(DbImport.Format.CSV);
        importCsv.importTables(csvOutput, table.getName());

        compareRows(rowsBeforeImport, sqlTemplate.query(SELECT_FROM_TEST_DB_IMPORT_1_ORDER_BY_ID));

    }    

    @Test
    public void testExportCsvToDirectory() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();

        DbImport importXml = new DbImport(platform);
        importXml.setFormat(DbImport.Format.XML);
        importXml.importTables(getClass().getResourceAsStream("/test-dbexportimport-3-tables.xml"));

        File dir = new File("target/test");
        FileUtils.deleteDirectory(dir);
        Assert.assertFalse(dir.exists());

        DbExport exportCsv = new DbExport(platform);
        exportCsv.setComments(true);
        exportCsv.setFormat(Format.CSV);
        exportCsv.setDir(dir.getAbsolutePath());
        exportCsv.exportTables(new String[] { "a", "b", "c" });

        Assert.assertTrue(dir.exists());
        Assert.assertTrue(dir.isDirectory());

        File a = new File(dir, platform.getTableFromCache("a", false).getName() + ".csv");
        Assert.assertTrue(a.exists());
        Assert.assertTrue(a.isFile());
        List<String> lines = FileUtils.readLines(a);
        Assert.assertEquals(9, lines.size());
        Assert.assertEquals("\"id\",\"string_value\"", lines.get(5));
        Assert.assertEquals("\"1\",\"This is a test of a\"", lines.get(6));
        Assert.assertEquals("\"2\",\"This is a test of a\"", lines.get(7));

        File b = new File(dir, platform.getTableFromCache("b", false).getName() + ".csv");
        Assert.assertTrue(b.exists());
        Assert.assertTrue(b.isFile());
        lines = FileUtils.readLines(b);
        Assert.assertEquals(10, lines.size());
        Assert.assertEquals("\"id\",\"string_value\"", lines.get(5));
        Assert.assertEquals("\"1\",\"This is a test of b\"", lines.get(6));
        Assert.assertEquals("\"2\",\"This is a test of b\"", lines.get(7));
        Assert.assertEquals("\"3\",\"This is line 3 of b\"", lines.get(8));

        File c = new File(dir, platform.getTableFromCache("c", false).getName() + ".csv");
        Assert.assertTrue(c.exists());
        Assert.assertTrue(c.isFile());
        lines = FileUtils.readLines(c);
        Assert.assertEquals(9, lines.size());
        Assert.assertEquals("\"id\",\"string_value\"", lines.get(5));
        Assert.assertEquals("\"1\",\"This is a test of c\"", lines.get(6));
        Assert.assertEquals("\"2\",\"This is a test of c\"", lines.get(7));

    }

    protected void compareRows(List<Row> one, List<Row> two) {
        if (one.size() != two.size()) {
            Assert.fail("First list had " + one.size() + " and second list had " + two.size());
        }
        for (int i = 0; i < one.size(); i++) {
            Row rOne = one.get(i);
            Row rTwo = two.get(i);
            Set<String> keys = rOne.keySet();
            for (String key : keys) {
                if (!ObjectUtils.equals(rOne.get(key), rTwo.get(key))) {
                    Assert.fail("The " + i + " element was not the same.  The column " + key
                            + " had a value of " + rOne.get(key) + " for one row and "
                            + rTwo.get(key) + " for the other");
                }
            }
        }
    }

    protected Row findInList(List<Row> rows, String pk, Object pkValue) {
        for (Row row : rows) {
            Object value = row.get(pk);
            if (ObjectUtils.equals(value, pkValue)) {
                return row;
            }
        }
        return null;
    }

}
