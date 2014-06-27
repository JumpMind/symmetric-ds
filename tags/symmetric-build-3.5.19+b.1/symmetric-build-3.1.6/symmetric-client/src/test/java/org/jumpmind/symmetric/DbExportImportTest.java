package org.jumpmind.symmetric;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.symmetric.DbExport.Compatible;
import org.jumpmind.symmetric.DbExport.Format;
import org.jumpmind.symmetric.io.data.writer.ConflictException;
import org.jumpmind.symmetric.service.impl.AbstractServiceTest;
import org.junit.Test;

public class DbExportImportTest extends AbstractServiceTest {

    @Test
    public void exportTableInAnotherSchemaOnH2() throws Exception {
        if (getPlatform().getName().equals(DatabaseNamesConstants.H2)) {
            ISymmetricEngine engine = getSymmetricEngine();
            DataSource ds = engine.getDataSource();
            ISqlTemplate template = getPlatform().getSqlTemplate();
            template.update("CREATE SCHEMA IF NOT EXISTS A");
            template.update("CREATE TABLE IF NOT EXISTS A.TEST (ID INT, NOTES VARCHAR(100), PRIMARY KEY (ID))");
            template.update("DELETE FROM A.TEST");
            template.update("INSERT INTO A.TEST VALUES(1,'test')");

            DbExport export = new DbExport(ds);
            export.setSchema("A");
            export.setFormat(Format.SQL);
            export.setNoCreateInfo(false);
            export.setNoData(false);

            String output = export.exportTables(new String[] { "TEST" }).toLowerCase();
            System.out.println(output);
            // TODO validate
        }
    }

    @Test
    public void exportTestDatabaseSQL() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        DataSource ds = engine.getDataSource();

        Table[] tables = engine.getSymmetricDialect().readSymmetricSchemaFromXml().getTables();

        DbExport export = new DbExport(ds);
        export.setFormat(Format.SQL);
        export.setNoCreateInfo(false);
        export.setNoData(true);
        export.setSchema(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultSchema());
        export.setCatalog(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultCatalog());
        export.setCompatible(Compatible.H2);
        String output = export.exportTables(tables).toLowerCase();

        Assert.assertEquals(output, 32, StringUtils.countMatches(output, "create table \"sym_"));
        Assert.assertEquals(30,
                StringUtils.countMatches(output, "varchar(" + Integer.MAX_VALUE + ")"));
    }

    @Test
    public void exportThenImportXml() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        DataSource ds = engine.getDataSource();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database testTables = platform.readDatabaseFromXml("/test-dbimport.xml", true);
        Table table = testTables.findTable("test_db_import_1", false);

        DbImport dbImport = new DbImport(ds);
        dbImport.setFormat(DbImport.Format.XML);
        dbImport.setSchema(platform.getDefaultSchema());
        dbImport.setCatalog(platform.getDefaultCatalog());
        dbImport.setAlterCaseToMatchDatabaseDefaultCase(true);
        dbImport.importTables(getClass().getResourceAsStream("/test-dbimport.xml"));

        DbExport export = new DbExport(ds);
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

    protected void recreateImportTable() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        DataSource ds = engine.getDataSource();
        DbImport reCreateTablesImport = new DbImport(ds);
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
        DataSource ds = engine.getDataSource();

        recreateImportTable();

        assertCountDbImportTableRecords(0);

        DbImport importCsv = new DbImport(ds);
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
        DataSource ds = engine.getDataSource();

        recreateImportTable();

        assertCountDbImportTableRecords(0);

        DbImport importCsv = new DbImport(ds);
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
    public void exportThenImportCsv() throws Exception {
        ISymmetricEngine engine = getSymmetricEngine();
        DataSource ds = engine.getDataSource();
        IDatabasePlatform platform = engine.getSymmetricDialect().getPlatform();
        Database testTables = platform.readDatabaseFromXml("/test-dbimport.xml", true);
        Table table = testTables.findTable("test_db_import_1", false);

        recreateImportTable();

        final int RECORD_COUNT = 100;

        DbFill fill = new DbFill(platform);
        fill.setRecordCount(RECORD_COUNT);
        fill.fillTables(table.getName());

        DbExport export = new DbExport(ds);
        export.setFormat(Format.CSV);
        export.setNoCreateInfo(true);
        export.setNoData(false);
        String csvOutput = export.exportTables(new String[] { table.getName() });

        recreateImportTable();

        DbImport importCsv = new DbImport(ds);
        importCsv.setFormat(DbImport.Format.CSV);
        importCsv.importTables(csvOutput, table.getName());

        DmlStatement dml = new DmlStatement(DmlType.COUNT, table.getCatalog(), table.getSchema(),
                table.getName(), null, table.getColumns(), false, null, null);
        Assert.assertEquals(RECORD_COUNT, platform.getSqlTemplate().queryForInt(dml.getSql()));

        // TODO test error

        // TODO test replace

        // TODO test ignore

        // TODO test force
    }    

}
