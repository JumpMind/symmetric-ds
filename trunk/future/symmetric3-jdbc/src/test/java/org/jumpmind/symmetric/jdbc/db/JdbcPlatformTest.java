package org.jumpmind.symmetric.jdbc.db;

import javax.sql.DataSource;

import junit.framework.Assert;

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
        IJdbcPlatform platform = JdbcPlatformFactory.createPlatform(dataSource);
        Assert.assertNotNull(platform);
    }
}
