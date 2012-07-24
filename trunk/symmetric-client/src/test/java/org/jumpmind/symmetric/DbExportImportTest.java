package org.jumpmind.symmetric;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.symmetric.DbExport.Compatible;
import org.jumpmind.symmetric.DbExport.Format;
import org.jumpmind.symmetric.service.impl.AbstractServiceTest;
import org.junit.Test;

public class DbExportImportTest extends AbstractServiceTest {

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
        Assert.assertEquals(29,
                StringUtils.countMatches(output, "varchar(" + Integer.MAX_VALUE + ")"));
    }

    @Test
    public void exportThenImport() throws Exception {
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
        String output = export.exportTables(new String[] {table.getName()});
        
        //System.out.println(output);
        // TODO validate

    }

}
