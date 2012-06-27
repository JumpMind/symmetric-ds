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
    public void exportTestDatabase() throws Exception {
        DataSource ds = getSymmetricEngine().getDataSource();
        DbExport export = new DbExport(ds);
        export.setFormat(Format.SQL);
        export.setNoCreateInfo(false);
        export.setNoData(true);
        export.setSchema(getSymmetricEngine().getSymmetricDialect().getPlatform().getDefaultSchema());
        export.setCatalog(getSymmetricEngine().getSymmetricDialect().getPlatform().getDefaultCatalog());
        export.setCompatible(Compatible.H2);
        String output = export.exportTables();
        Assert.assertEquals(output, 64, StringUtils.countMatches(output, "CREATE TABLE"));
        Assert.assertEquals(output, 37, StringUtils.countMatches(output, "CLOB"));
    }
    
}
