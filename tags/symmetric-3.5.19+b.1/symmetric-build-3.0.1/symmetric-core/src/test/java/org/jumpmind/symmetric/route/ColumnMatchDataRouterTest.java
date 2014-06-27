package org.jumpmind.symmetric.route;

import java.util.List;

import junit.framework.Assert;

import org.jumpmind.symmetric.route.ColumnMatchDataRouter.Expression;
import org.junit.Test;

public class ColumnMatchDataRouterTest {

    @Test
    public void testExpressionUsingLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two\ntwo=three\rthree!=:EXTERNAL_ID");
        Assert.assertEquals(3, expressions.size());
        Assert.assertEquals("two",expressions.get(0).tokens[1]);
        Assert.assertEquals("three",expressions.get(2).tokens[0]);
        Assert.assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionOrParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two OR two=three or three!=:EXTERNAL_ID");
        Assert.assertEquals(3, expressions.size());
        Assert.assertEquals("two",expressions.get(0).tokens[1]);
        Assert.assertEquals("three",expressions.get(2).tokens[0]);
        Assert.assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionOrAndLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two OR two=three\n\ror   three!=:EXTERNAL_ID");
        Assert.assertEquals(3, expressions.size());
        Assert.assertEquals("two",expressions.get(0).tokens[1]);
        Assert.assertEquals("three",expressions.get(2).tokens[0]);
        Assert.assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionWithOrInColumnName() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("ORDER_ID=:EXTERNAL_ID");
        Assert.assertEquals(1, expressions.size());
        Assert.assertEquals("ORDER_ID",expressions.get(0).tokens[0]);
        Assert.assertEquals(":EXTERNAL_ID",expressions.get(0).tokens[1]);
    }
    
}
