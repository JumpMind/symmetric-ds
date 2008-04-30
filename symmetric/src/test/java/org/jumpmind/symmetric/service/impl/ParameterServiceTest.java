package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.PropertiesConstants;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ParameterServiceTest extends AbstractDatabaseTest {

   
    @Test(groups="continuous")
    public void testParameterGetFromDefaults() {
       Assert.assertEquals(getParameterService().getString(PropertiesConstants.RUNTIME_CONFIG_TABLE_PREFIX), "sym", "Unexpected default table prefix found.");
    }
}
