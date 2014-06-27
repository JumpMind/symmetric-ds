package org.jumpmind.symmetric.db.hsqldb;

import java.io.File;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

import org.hsqldb.jdbcDriver;
import org.junit.Test;

public class HsqldbTriggerUnitTest {

    @Test
    public void testFindConnection() throws Exception {
        File dir = new File("target/test");
        if (dir.exists()) {
            dir.delete();
        }
        String testTable = "test";
        Class.forName(jdbcDriver.class.getName());
        Connection c = DriverManager.getConnection("jdbc:hsqldb:file:target/test/testdb", "sa", "");
        Statement stmt = c.createStatement();
        stmt.executeUpdate("create table " + testTable + " (node_group_id varchar(100))");
        Connection c2 = new HsqlDbTrigger().findConnection(testTable);
        Statement stmt2 = c2.createStatement();
        stmt2.executeQuery("select count(*) from " + testTable);
        stmt2.close();
        stmt.close();
        c2.close();
        c.close();
    }

}
