package org.jumpmind.symmetric.test;

import java.sql.Types;

import junit.framework.Assert;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.model.Node;

public class WildcardTest extends AbstractTest {

    @Override
    protected Table[] getTables(String name) {
        Table a = new Table("a");
        a.addColumn(new Column("id", true, Types.INTEGER, -1, -1));
        a.addColumn(new Column("notes", false, Types.VARCHAR, 255, -1));

        Table b = new Table("b");
        b.addColumn(new Column("id", true, Types.INTEGER, -1, -1));
        b.addColumn(new Column("notes", false, Types.VARCHAR, 255, -1));

        Table c = new Table("c");
        c.addColumn(new Column("id", true, Types.INTEGER, -1, -1));
        c.addColumn(new Column("notes", false, Types.VARCHAR, 255, -1));

        return new Table[] { a, b, c };
    }

    @Override
    protected void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer) throws Exception {

        for (int i = 0; i < 10; i++) {
            rootServer.getSqlTemplate().update("insert into a (id, notes) values(?,?)", i,
                    "test 12345");
        }

        for (int i = 0; i < 100; i++) {
            rootServer.getSqlTemplate().update("insert into b (id, notes) values(?,?)", i,
                    "test 54321");
        }

        for (int i = 0; i < 5; i++) {
            rootServer.getSqlTemplate().update("insert into c (id, notes) values(?,?)", i,
                    "no sync");
        }

        loadConfigAtRegistrationServer();

        Assert.assertEquals(0, clientServer.getSqlTemplate().queryForInt("select count(*) from a"));
        Assert.assertEquals(0, clientServer.getSqlTemplate().queryForInt("select count(*) from b"));
        Assert.assertEquals(0, clientServer.getSqlTemplate().queryForInt("select count(*) from c"));

        rootServer.openRegistration("client", "client");

        pull("client");

        Node clientNode = clientServer.getNodeService().findIdentity();
        Assert.assertNotNull(clientNode);

        rootServer.getDataService().reloadNode(clientNode.getNodeId(), false, "unit test");

        pull("client");

        // load succeeded
        Assert.assertEquals(10, clientServer.getSqlTemplate().queryForInt("select count(*) from a"));
        Assert.assertEquals(100, clientServer.getSqlTemplate()
                .queryForInt("select count(*) from b"));
        // c was excluded
        Assert.assertEquals(0, clientServer.getSqlTemplate().queryForInt("select count(*) from c"));

        for (int i = 100; i < 200; i++) {
            rootServer.getSqlTemplate().update("insert into b (id, notes) values(?,?)", i,
                    "test 54321");
        }
        
        for (int i = 5; i < 10; i++) {
            rootServer.getSqlTemplate().update("insert into c (id, notes) values(?,?)", i,
                    "no sync");
        }

        pull("client");

        Assert.assertEquals(200, clientServer.getSqlTemplate()
                .queryForInt("select count(*) from b"));
        Assert.assertEquals(0, clientServer.getSqlTemplate().queryForInt("select count(*) from c"));

    }


}
