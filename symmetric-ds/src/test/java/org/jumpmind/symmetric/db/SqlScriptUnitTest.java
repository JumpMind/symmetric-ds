package org.jumpmind.symmetric.db;

import java.sql.Connection;
import java.sql.DriverManager;

import junit.framework.Assert;

import org.junit.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class SqlScriptUnitTest {

    @Test
    public void testSimpleSqlScript() throws Exception {
        SingleConnectionDataSource ds = getDataSource();
        SqlScript script = new SqlScript(getClass().getResource("sqlscript-simple.sql"), ds);
        script.execute();
        JdbcTemplate template = new JdbcTemplate(ds);
        Assert.assertEquals(2, template.queryForInt("select count(*) from test"));
        Assert.assertEquals(3, template.queryForObject("select test from test where test_id=2", String.class).split("\r\n|\r|\n").length);
        ds.destroy();
    }
    
    private SingleConnectionDataSource getDataSource() throws Exception {
        Connection c = DriverManager.getConnection("jdbc:h2:mem:sqlscript");
        return new SingleConnectionDataSource(c, true);
    }
}
