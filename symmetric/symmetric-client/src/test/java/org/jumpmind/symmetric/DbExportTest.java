package org.jumpmind.symmetric;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.DbExport.Compatible;
import org.jumpmind.symmetric.DbExport.Format;
import org.jumpmind.symmetric.service.impl.AbstractServiceTest;
import org.junit.Test;

public class DbExportTest extends AbstractServiceTest {

    @Test
    public void exportTestDatabaseSQL() throws Exception {
        DataSource ds = getSymmetricEngine().getDataSource();
        DbExport export = new DbExport(ds);
        export.setFormat(Format.SQL);
        export.setNoCreateInfo(false);
        export.setNoData(true);
        export.setSchema(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultSchema());
        export.setCatalog(getSymmetricEngine().getSymmetricDialect().getPlatform()
                .getDefaultCatalog());
        export.setCompatible(Compatible.H2);
        String output = export.exportTables().toLowerCase();
        Assert.assertEquals(output, 31, StringUtils.countMatches(output, "create table \"sym_")
                - StringUtils.countMatches(output, "create table \"sym_on_"));
        int longvarcharCount = StringUtils.countMatches(output, "varchar(" + Integer.MAX_VALUE
                + ")");
        // the count may be 34 on platforms that map longvarchar to longvarchar and clob to clob
        // it will be 35 on platforms that map clob to longvarchar
        Assert.assertTrue("The count was " + longvarcharCount
                + ". It should have been > 32.  The sql generated was:\n" + output,
                longvarcharCount >= 32);
    }

}
