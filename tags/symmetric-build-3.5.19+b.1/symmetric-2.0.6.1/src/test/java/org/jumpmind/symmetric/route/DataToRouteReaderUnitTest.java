package org.jumpmind.symmetric.route;

import java.util.Map;

import javax.annotation.Resource;

import org.jumpmind.symmetric.model.Channel;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/org/jumpmind/symmetric/service/impl/router-service-sql.xml" })
public class DataToRouteReaderUnitTest {
    
    private static final String BLANK = "''";
    
    @Resource
    Map<String,String> routerServiceSql;
    
    @Test
    public void testOldDataReplacement() {
        DataToRouteReader reader = new DataToRouteReader(null, 1, routerServiceSql, 1000, null, null, null);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(channel).contains("old_data"));
        Assert.assertFalse(reader.getSql(channel).contains(BLANK));
        channel.setUseOldDataToRoute(false);
        Assert.assertFalse(reader.getSql(channel).contains("old_data"));
        Assert.assertTrue(reader.getSql(channel).contains(BLANK));
    }
    
    @Test
    public void testRowDataReplacement() {
        DataToRouteReader reader = new DataToRouteReader(null, 1, routerServiceSql, 1000, null, null, null);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(channel).contains("row_data"));
        Assert.assertFalse(reader.getSql(channel).contains(BLANK));
        channel.setUseRowDataToRoute(false);
        Assert.assertFalse(reader.getSql(channel).contains("row_data"));
        Assert.assertTrue(reader.getSql(channel).contains(BLANK));
    }   
    
    @Test
    public void testOldAndRowDataReplacement() {
        DataToRouteReader reader = new DataToRouteReader(null, 1, routerServiceSql, 1000, null, null, null);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(channel).contains("row_data"));
        Assert.assertTrue(reader.getSql(channel).contains("old_data"));
        Assert.assertFalse(reader.getSql(channel).contains(BLANK));
        channel.setUseRowDataToRoute(false);
        channel.setUseOldDataToRoute(false);
        Assert.assertFalse(reader.getSql(channel).contains("row_data"));
        Assert.assertFalse(reader.getSql(channel).contains("old_data"));
        Assert.assertTrue(reader.getSql(channel).contains(BLANK));
    }  

}
