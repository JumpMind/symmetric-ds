package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.io.FileUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.jdbc.datasource.DriverDataSourceProperties;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcPlatformTest {

    protected static DataSource dataSource;

    @BeforeClass
    public static void setupDataSource() {
        FileUtils.deleteDirectory("target/h2");
        dataSource = new DriverDataSourceProperties("src/test/resources/test-jdbc.properties")
                .getDataSource();
    }

    @Test
    public void testJdbcPlatformFactory() {
        IJdbcPlatform platform = getPlatform();
        Assert.assertNotNull(platform);
    }
    
    protected IJdbcPlatform getPlatform() {
        return JdbcDbPlatformFactory.createPlatform(dataSource);
    }
    
    @Test
    public void testCreateTable() {
        IJdbcPlatform platform = getPlatform();
        Table table = new Table("test",
                new Column("test_id", TypeMap.NUMERIC, "10,2", false, true, true));
        ISqlConnection sqlConnection = platform.getSqlConnection();
        String alterSql = platform.getAlterScriptFor(table);
        Assert.assertFalse(StringUtils.isBlank(alterSql));
        sqlConnection.update(alterSql);
        alterSql = platform.getAlterScriptFor(table);
        Assert.assertTrue("There should have been no changes to the table.  Instead, we received the alter script: " + alterSql,StringUtils.isBlank(alterSql));
    }
}
