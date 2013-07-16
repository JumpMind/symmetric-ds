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
        List<Expression> expressions = router.parse("one=door OR two=three or three!=:EXTERNAL_ID");
        Assert.assertEquals(3, expressions.size());
        Assert.assertEquals("door",expressions.get(0).tokens[1]);
        Assert.assertEquals("three",expressions.get(2).tokens[0]);
        Assert.assertEquals(false,expressions.get(2).equals);
    }
    
    @Test
    public void testExpressionTickParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one='two three' OR four='five'\r\nor six=isn't \r\n seven='can''t'" +
                                                    " or eight='yall \n nine=' ten  ' or eleven  =  'twelve'  ");
        Assert.assertEquals(7, expressions.size());

        Assert.assertEquals("one",expressions.get(0).tokens[0]);
        Assert.assertEquals("two three",expressions.get(0).tokens[1]);

        Assert.assertEquals("four",expressions.get(1).tokens[0]);
        Assert.assertEquals("five",expressions.get(1).tokens[1]);

        Assert.assertEquals("six",expressions.get(2).tokens[0]);
        Assert.assertEquals("isn't",expressions.get(2).tokens[1]);

        Assert.assertEquals("seven",expressions.get(3).tokens[0]);
        Assert.assertEquals("can't",expressions.get(3).tokens[1]);

        Assert.assertEquals("eight",expressions.get(4).tokens[0]);
        Assert.assertEquals("'yall",expressions.get(4).tokens[1]);

        Assert.assertEquals("nine",expressions.get(5).tokens[0]);
        Assert.assertEquals(" ten  ",expressions.get(5).tokens[1]);

        Assert.assertEquals("eleven",expressions.get(6).tokens[0]);
        Assert.assertEquals("twelve",expressions.get(6).tokens[1]);


    }
    
    @Test
    public void testExpressionOrAndLineFeedsParsing() {
        ColumnMatchDataRouter router = new ColumnMatchDataRouter();
        List<Expression> expressions = router.parse("one=two OR three=four\r\nor   five!=:EXTERNAL_ID");
        Assert.assertEquals(3, expressions.size());
        Assert.assertEquals("two",expressions.get(0).tokens[1]);
        Assert.assertEquals("three",expressions.get(1).tokens[0]);
        Assert.assertEquals("five",expressions.get(2).tokens[0]);
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
