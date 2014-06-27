package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.test.TestConstants;
import org.junit.Assert;
import org.junit.Test;

public class ParameterServiceTest extends AbstractDatabaseTest {

    public ParameterServiceTest() throws Exception {
        super();
    }

    public ParameterServiceTest(String dbName) {
        super(dbName);
    }

    @Test
    public void testParameterGetFromDefaults() {
        Assert.assertEquals("Unexpected default table prefix found.", getParameterService().getString(ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX), "sym");
    }

    @Test
    public void testParameterGetFromDatabase() {
        Assert.assertEquals(getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS), 2);
        getParameterService().saveParameter(TestConstants.TEST_CLIENT_EXTERNAL_ID,
                TestConstants.TEST_CLIENT_NODE_GROUP, ParameterConstants.CONCURRENT_WORKERS, 10);

        // make sure we are not picking up someone else's parameter
        Assert.assertEquals(getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS), 2);

        getParameterService().saveParameter(getParameterService().getExternalId(), getParameterService().getNodeGroupId(),
                ParameterConstants.CONCURRENT_WORKERS, 5);               

        Assert.assertEquals(5, getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS));

        getParameterService().saveParameter(ParameterConstants.CONCURRENT_WORKERS, 10);

        Assert.assertEquals(getParameterService().getInt(ParameterConstants.CONCURRENT_WORKERS), 10);
    }

    @Test
    public void testBooleanParameter() {
        Assert.assertEquals(getParameterService().is("boolean.test"), false);
        getParameterService().saveParameter("boolean.test", true);
        Assert.assertEquals(getParameterService().is("boolean.test"), true);
    }

}
