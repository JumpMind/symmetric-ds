package org.jumpmind.symmetric.core.db;

import java.sql.Types;

import junit.framework.Assert;

import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Table;
import org.junit.Test;

public class QueryTest {

    final static Table TABLE1 = new Table("table1", new Column("column1", Types.INTEGER),
            new Column("column2", Types.VARCHAR));
    
    final static Table TABLE2 = new Table("table2", new Column("column1", Types.INTEGER),
            new Column("text", Types.VARCHAR));


    @Test
    public void testSelectFromTable1() {
        Object[] args = new Object[] { 1, "someValue" };
        Query query = Query.create("", TABLE1).where(TABLE1.getColumn(0), "=", args[0])
                .and(TABLE1.getColumn(1), "!=", args[1]);
        Assert.assertEquals(
                "select t.column1, t.column2 from table1 t where t.column1=? and t.column2!=?",
                query.getSql());

        Assert.assertEquals(args[0], query.getArgs()[0]);
        Assert.assertEquals(args[1], query.getArgs()[1]);

        Assert.assertEquals(Types.INTEGER, query.getArgTypes()[0]);
        Assert.assertEquals(Types.VARCHAR, query.getArgTypes()[1]);
    }
    
    @Test
    public void testSelectWithJoin() {
        Object[] args = new Object[] { 1, "someValue" };
        Query query = Query.create("", TABLE1, TABLE2).where(TABLE1.getColumn(0), "=", args[0])
                .and(TABLE2.getColumn(1), "!=", args[1]);
        Assert.assertEquals(
                "select t1.column1, t1.column2, t2.column1, t2.text from table1 t1 inner join table2 t2 on t1.column1=t2.column1 where t1.column1=? and t2.text!=?",
                query.getSql());

        Assert.assertEquals(args[0], query.getArgs()[0]);
        Assert.assertEquals(args[1], query.getArgs()[1]);

        Assert.assertEquals(Types.INTEGER, query.getArgTypes()[0]);
        Assert.assertEquals(Types.VARCHAR, query.getArgTypes()[1]);
    }
}
