package org.jumpmind.symmetric;

import junit.framework.Assert;

import org.jumpmind.symmetric.ext.TestDataLoaderFilter;
import org.jumpmind.symmetric.test.DatabaseRole;
import org.jumpmind.symmetric.test.DatabaseTestSuite;
import org.jumpmind.symmetric.test.TestSetupUtil;
import org.junit.Test;

public class SymmetricLauncherUnitTest {

    @Test
    public void testStartServer() throws Exception {
        String propsFile = TestSetupUtil.writeTempPropertiesFileFor(DatabaseTestSuite.DEFAULT_TEST_PREFIX, "h2", DatabaseRole.ROOT).getAbsolutePath();
        SymmetricLauncher.join = false;
        SymmetricLauncher.main("-p",propsFile,"-S");
        Assert.assertNull(SymmetricLauncher.exception);
        Assert.assertEquals(2, TestDataLoaderFilter.getNumberOfTimesCreated());
        Assert.assertNotNull(SymmetricLauncher.webServer);
        SymmetricLauncher.webServer.stop();
    }
    
}
