package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.TypeMap;
import org.jumpmind.symmetric.jdbc.datasource.DriverDataSourceProperties;
import org.junit.BeforeClass;
import org.junit.Test;

public class JdbcPlatformTest {

    protected static DataSource dataSource;

    @BeforeClass
    public static void setupDataSource() {
        dataSource = new DriverDataSourceProperties("src/test/resources/test-jdbc.properties")
                .getDataSource();
    }

    @Test
    public void testJdbcPlatformFactory() {
        IJdbcPlatform platform = getPlatform();
        Assert.assertNotNull(platform);
    }
    
    protected IJdbcPlatform getPlatform() {
        return JdbcPlatformFactory.createPlatform(dataSource);
    }
    
    @Test
    public void testCreateTable() {
        IJdbcPlatform platform = getPlatform();
        Table table = new Table("test",
                new Column("test_id", TypeMap.NUMERIC, "10,2", false, true));
        System.out.println(platform.getAlterScriptFor(table));
    }
}
