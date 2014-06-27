package org.jumpmind.symmetric.config;

import junit.framework.Assert;

import org.jumpmind.symmetric.common.Constants;
import org.junit.Test;

public class DynamicPropertiesFilesUnitTest {

    @Test
    public void testOverrideProperties1() {
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, "test.properties");
        DynamicPropertiesFiles dpFiles = new DynamicPropertiesFiles();
        Assert.assertEquals(1, dpFiles.size());
        Assert.assertTrue(dpFiles.contains("test.properties"));
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_1);
    }
    
    @Test
    public void testOverrideProperties1and2() {
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, "test1.properties");
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_2, "test2.properties");
        DynamicPropertiesFiles dpFiles = new DynamicPropertiesFiles();
        Assert.assertEquals(2, dpFiles.size());
        Assert.assertTrue(dpFiles.contains("test1.properties"));
        Assert.assertTrue(dpFiles.contains("test2.properties"));
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_2);
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_1);
    }
    
    @Test
    public void testOverrideProperties3andTemp() {
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_PREFIX + "3", "test3.properties");
        System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_TEMP, "test.temp.properties");
        DynamicPropertiesFiles dpFiles = new DynamicPropertiesFiles();
        Assert.assertEquals(2, dpFiles.size());
        Assert.assertTrue(dpFiles.contains("test3.properties"));
        Assert.assertTrue(dpFiles.contains("test.temp.properties"));
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_PREFIX + "3");
        System.getProperties().remove(Constants.OVERRIDE_PROPERTIES_FILE_TEMP);
    }

}
