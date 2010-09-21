package org.jumpmind.symmetric.route;

import java.util.Map;

import javax.annotation.Resource;

import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.service.ISqlProvider;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/org/jumpmind/symmetric/service/impl/router-service-sql.xml" })
public class DataRefRouteReaderUnitTest {
    
    private static final String BLANK = "''";
    
    @Resource
    Map<String,String> routerServiceSql;
    
    @Test
    public void testOldDataReplacement() {
        DataRefRouteReader reader = new DataRefRouteReader(null, -1, 1, getSqlProvider(), 1000, null, null, false);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL, channel).contains("old_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains(BLANK));
        channel.setUseOldDataToRoute(false);
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("old_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains(BLANK));
    }
    
    @Test
    public void testRowDataReplacement() {
        DataRefRouteReader reader = new DataRefRouteReader(null, -1, 1, getSqlProvider(), 1000, null, null, false);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("row_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains(BLANK));
        channel.setUseRowDataToRoute(false);
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("row_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains(BLANK));
    }   
    
    @Test
    public void testOldAndRowDataReplacement() {
        DataRefRouteReader reader = new DataRefRouteReader(null, -1, 1, getSqlProvider(), 1000, null, null, false);
        Channel channel = new Channel();
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("row_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("old_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains(BLANK));
        channel.setUseRowDataToRoute(false);
        channel.setUseOldDataToRoute(false);
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("row_data"));
        Assert.assertFalse(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains("old_data"));
        Assert.assertTrue(reader.getSql(DataRefRouteReader.SELECT_DATA_TO_BATCH_SQL,channel).contains(BLANK));
    }  
    
    private ISqlProvider getSqlProvider() {
        return new ISqlProvider() {
            
            public String getSql(String key) {
                return routerServiceSql.get(key);
            }
        };
    }

}
