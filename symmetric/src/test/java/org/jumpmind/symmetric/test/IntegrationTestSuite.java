package org.jumpmind.symmetric.test;

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(ParameterizedSuite.class)
@SuiteClasses( { SimpleIntegrationTest.class, CleanupTest.class })
public class IntegrationTestSuite {

    static final String TEST_PREFIX = "test";

    @Parameters
    public static Collection<String[]> lookupClientServerDatabases() {
        return TestSetupUtil.lookupDatabasePairs(TEST_PREFIX);
    }

    String root;
    String client;

    public IntegrationTestSuite(String client, String root) {
        this.client = client;
        this.root = root;
    }

    @Test
    public void setup() throws Exception {
        TestSetupUtil.setup(TEST_PREFIX, TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT, client, root);
    }

}
