package org.jumpmind.symmetric.test;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.junit.Test;

public class CleanupTest {
    static final Log logger = LogFactory.getLog(SimpleIntegrationTest.class);

    String client;
    String root;

    public CleanupTest(String client, String root) throws Exception {
        this.client = client;
        this.root = root;
    }
    
    public CleanupTest(String client) throws Exception {
        this.client = null;
    }

    @Test
    public void cleanup() throws Exception {
        TestSetupUtil.cleanup();
    }
}
