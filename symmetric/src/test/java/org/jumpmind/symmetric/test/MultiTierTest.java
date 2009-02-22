package org.jumpmind.symmetric.test;

import java.io.File;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.RegistrationFailedException;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/multi-tier-test.xml" })
public class MultiTierTest {

    static final Log logger = LogFactory.getLog(MultiTierTest.class);

    @Resource
    protected SymmetricWebServer homeServer;

    @Resource
    protected SymmetricWebServer region01Server;

    @Resource
    protected SymmetricWebServer region02Server;

    @Resource
    protected SymmetricWebServer workstation000101;

    @Resource
    protected SymmetricWebServer workstation000102;

    @Resource
    protected Map<String, String> unitTestSql;

    @BeforeClass
    public static void setup() throws Exception {
        logger.info("Setting up the multi-tiered test");
        File databaseDir = new File("target/multi-tier");
        FileUtils.deleteDirectory(new File("target/multi-tier"));
        logger.info("Just deleted " + databaseDir.getAbsolutePath());
    }

    @Test
    public void validateHomeServerStartup() {
        Assert.assertTrue(homeServer.getEngine().isStarted());
        INodeService nodeService = AppUtils.find(Constants.NODE_SERVICE,
                homeServer);
        Node node = nodeService.findIdentity();
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getNodeId(),
                MultiTierTestConstants.HOME_NODE_ID);
    }

    @Test
    public void attemptToLoadWorkstation000101BeforeRegion01IsRegistered() {
        try {
            workstation000101.getEngine().pull();
            Assert.fail("Registration should have failed.");
        } catch (RegistrationFailedException ex) {

        }
    }

    @Test
    public void registerAndLoadRegion01() {

    }

    @Test
    public void registerAndLoadRegion02() {

    }

    @Test
    public void registerAndLoadWorkstation000101DirectlyWithRegion01() {

    }

    @Test
    public void registerAndLoadWorkstation000102WithHomeServer() {

    }

    @Test
    public void sendDataFromHomeToAllWorkstations() {

    }

    @Test
    public void sendDataFromHomeToWorkstation000101Only() {

    }

}
