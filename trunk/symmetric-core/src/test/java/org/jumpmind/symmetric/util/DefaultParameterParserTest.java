package org.jumpmind.symmetric.util;

import java.util.Map;

import junit.framework.Assert;

import org.jumpmind.properties.DefaultParameterParser;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.junit.Test;

public class DefaultParameterParserTest {

    @Test
    public void testParse() {
        DefaultParameterParser parser = new DefaultParameterParser("/symmetric-default.properties");
        Map<String, ParameterMetaData> metaData = parser.parse();
        
        Assert.assertNotNull(metaData);
        Assert.assertTrue(metaData.size() > 0);
        ParameterMetaData meta = metaData.get(ParameterConstants.PARAMETER_REFRESH_PERIOD_IN_MS);
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.getDescription().length() > 0);
        Assert.assertTrue(meta.isDatabaseOverridable());

        meta = metaData.get(ParameterConstants.NODE_GROUP_ID);
        Assert.assertNotNull(meta);
        Assert.assertTrue(meta.getDescription().length() > 0);
        Assert.assertFalse(meta.isDatabaseOverridable());

    
    }
}
