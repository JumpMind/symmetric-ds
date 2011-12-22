package org.jumpmind.util;

import junit.framework.Assert;

import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.junit.Test;

public class EnvironmentSpecificPropertiesTest {
    
    @Test
    public void testEnvironmentSpecificProperties() throws Exception {
        EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties("environment");
        Assert.assertEquals(0, properties.size());
        properties.load(getClass().getResourceAsStream("/test.env.specifc.properties"));
        Assert.assertEquals(2, properties.size());
        Assert.assertEquals("one", properties.get("name1"));
        Assert.assertEquals("two", properties.get("name2"));
    }

}
