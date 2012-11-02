package org.jumpmind.util;

import junit.framework.Assert;

import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.junit.Test;

public class EnvironmentSpecificPropertiesTest {
    
    @Test
    public void testEnvironmentSpecificProperties() throws Exception {
        EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(getClass().getResource("/test.env.specifc.properties"), "environment");
        Assert.assertEquals(4, properties.size()-System.getProperties().size());
        Assert.assertEquals("one", properties.get("name1"));
        Assert.assertEquals("two", properties.get("name2"));
    }

}
