package org.jumpmind.symmetric.db.h2;

import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.Assert;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.PlatformFactory;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Database;
import org.apache.ddlutils.model.Table;
import org.h2.Driver;
import org.junit.Test;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class H2PlatformUnitTest {

    private static final String H2 = "H2";

    @Test
    public void testColumnSizeChange() throws Exception {
        Platform pf = getPlatform();
        Database db = new Database();
        Table table = new Table();
        table.setName("TEST_TABLE");
        Column column1 = new Column();
        column1.setName("COLUMN_1");
        column1.setType("VARCHAR");
        column1.setSize("20");
        table.addColumn(column1);
        db.addTable(table);
        pf.createTables(db, true, true);
        
        SimpleJdbcTemplate jdbcTemplate = new SimpleJdbcTemplate(pf.getDataSource());
        Assert.assertEquals(0, jdbcTemplate.queryForInt("select count(*) from TEST_TABLE"));
        Assert.assertEquals(20, jdbcTemplate.queryForInt("select NUMERIC_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE' and COLUMN_NAME='COLUMN_1'"));
        
        column1.setSize("50");
        pf.alterTables(db, true);
        Assert.assertEquals(50, jdbcTemplate.queryForInt("select NUMERIC_PRECISION from INFORMATION_SCHEMA.COLUMNS where TABLE_NAME='TEST_TABLE' and COLUMN_NAME='COLUMN_1'"));
    }
    
    protected Platform getPlatform() throws Exception {
        PlatformFactory.registerPlatform(H2, H2Platform.class);
        Platform pf = PlatformFactory.createNewPlatformInstance(H2);
        Class.forName(Driver.class.getName());
        Connection c = DriverManager.getConnection("jdbc:h2:mem:test", "test", "test");
        pf.setDataSource(new SingleConnectionDataSource(c, true));
        return pf;
    }
}
