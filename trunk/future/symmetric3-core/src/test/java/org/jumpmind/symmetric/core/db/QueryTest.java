package org.jumpmind.symmetric.core.db;

import java.sql.Types;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;
import org.junit.Test;

public class QueryTest {

    final static Table TABLE1 = new Table("table1", new Column("column1"), new Column("column2"));

    @Test
    public void testOneTableSelect() {
        Object[] args = new Object[] { 1, "someValue" };
        Query query = Query.create(TABLE1).where(TABLE1.getColumn(0), "=", args[0])
                .and(TABLE1.getColumn(1), "!=", args[1]);
        Assert.assertEquals("select column1, column2 from table1 where column1=? and column2!=?",
                query.getSql());

        Assert.assertEquals(args[0], query.getArgs()[0]);
        Assert.assertEquals(args[1], query.getArgs()[1]);

        Assert.assertEquals(Types.INTEGER, query.getArgTypes()[0]);
        Assert.assertEquals(Types.VARCHAR, query.getArgTypes()[1]);
    }
}
