
package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.AbstractTest;
import org.jumpmind.symmetric.common.Constants;
import org.testng.annotations.Test;

public class PurgeServiceTest extends AbstractTest
{
    @Test(groups="continuous")
    public void testThatPurgeExecutes()
    {
        PurgeService service = (PurgeService) getBeanFactory().getBean(Constants.PURGE_SERVICE);        
        service.purge();
    }
}
