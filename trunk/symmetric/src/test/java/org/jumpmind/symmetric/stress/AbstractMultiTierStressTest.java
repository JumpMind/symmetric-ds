package org.jumpmind.symmetric.stress;

import java.io.File;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.RegistrationFailedException;
import org.jumpmind.symmetric.test.MultiTierTestConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
abstract public class AbstractMultiTierStressTest {

    final static Log logger = LogFactory
            .getLog(AbstractMultiTierStressTest.class);

    @Resource
    protected SymmetricWebServer homeServer;

    @Resource
    protected SymmetricWebServer regionServer;

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
                MultiTierTestConstants.NODE_ID_HOME);
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
    public void registerAndLoadRegion() {
        registerAndLoad(homeServer, MultiTierTestConstants.NODE_ID_REGION_1,
                regionServer, MultiTierTestConstants.NODE_GROUP_REGION, false,
                false, true);
    }

    @Test
    public void registerAndLoadWorkstations() {
        registerAndLoad(regionServer,
                MultiTierTestConstants.NODE_ID_STORE_0001_WORKSTATION_001,
                workstation000101,
                MultiTierTestConstants.NODE_GROUP_WORKSTATION, true, true, true);
        registerAndLoad(regionServer,
                MultiTierTestConstants.NODE_ID_STORE_0001_WORKSTATION_002,
                workstation000102,
                MultiTierTestConstants.NODE_GROUP_WORKSTATION, true, true, true);
    }

    @Test
    public void stressTest() {
        PushThread w1 = new PushThread(unitTestSql
                .get("insertWorkstationToHomeSql"), workstation000101, 100, 25,
                10);
        PushThread w2 = new PushThread(unitTestSql
                .get("insertWorkstationToHomeSql"), workstation000102, 100, 25,
                12);
        PushThread r1 = new PushThread(null, regionServer, 1000, 25, 15);
        r1.start();
        w2.start();
        w1.start();
        while (!r1.done || !w2.done || !w1.done) {
            try {
                Thread.sleep(100);
            } catch (Exception ex) {
            }
        }
        
        JdbcTemplate t = getTemplate(homeServer);
        int countAtHomeServer = t.queryForInt("select count(*) from sync_workstation_to_home");
        int countInserted = r1.insertedCount + w1.insertedCount + w2.insertedCount;
        Assert.assertTrue(countInserted > 0);
        Assert.assertEquals(countInserted, countAtHomeServer);
    }

    protected void registerAndLoad(SymmetricWebServer registrationServer,
            String externalId, SymmetricWebServer clientNode,
            String nodeGroupId, boolean autoRegister, boolean autoReload,
            boolean testReload) {
        IParameterService parameterService = getParameterService(registrationServer);
        if (!autoRegister) {
            registrationServer.getEngine().openRegistration(nodeGroupId,
                    externalId);
            parameterService.saveParameter(
                    ParameterConstants.AUTO_REGISTER_ENABLED, false);
        } else {
            parameterService.saveParameter(
                    ParameterConstants.AUTO_REGISTER_ENABLED, true);
        }
        clientNode.getEngine().pull();
        INodeService nodeService = AppUtils.find(Constants.NODE_SERVICE,
                clientNode);
        Node node = nodeService.findIdentity();
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getNodeId(), externalId);

        if (testReload) {
            if (!autoReload) {
                registrationServer.getEngine().reloadNode(externalId);
                parameterService.saveParameter(
                        ParameterConstants.AUTO_RELOAD_ENABLED, false);
            } else {
                parameterService.saveParameter(
                        ParameterConstants.AUTO_RELOAD_ENABLED, true);
            }

            IOutgoingBatchService homeOutgoingBatchService = AppUtils.find(
                    Constants.OUTGOING_BATCH_SERVICE, registrationServer);
            while (!homeOutgoingBatchService.isInitialLoadComplete(externalId)) {
                clientNode.getEngine().pull();
            }

            NodeSecurity clientNodeSecurity = nodeService
                    .findNodeSecurity(externalId);
            Assert.assertFalse(clientNodeSecurity.isInitialLoadEnabled());
            Assert.assertNotNull(clientNodeSecurity.getInitialLoadTime());
        }
    }

    protected JdbcTemplate getTemplate(SymmetricWebServer server) {
        JdbcTemplate t = AppUtils.find(Constants.JDBC, server.getEngine());
        return t;
    }

    protected IParameterService getParameterService(SymmetricWebServer server) {
        IParameterService s = AppUtils
                .find(Constants.PARAMETER_SERVICE, server);
        return s;
    }

    class PushThread extends Thread {
        SymmetricWebServer client;
        String insertSql;
        int insertedCount;
        int numberOfIterations;
        int numberOfInsertsPerIteration;
        boolean done = false;
        long sleep;

        public PushThread(String insertSql, SymmetricWebServer client,
                int numberOfIterations, int numberOfInsertsPerIteration,
                long sleep) {
            this.setName("stressthread" + getParameterService(client).getExternalId());
            this.insertSql = insertSql;
            this.numberOfInsertsPerIteration = numberOfInsertsPerIteration;
            this.numberOfIterations = numberOfIterations;
            this.client = client;
            this.sleep = sleep;
        }

        @Override
        public void run() {
            IParameterService ps = getParameterService(client);
            for (int i = 0; i < numberOfIterations; i++) {
                if (insertSql != null) {
                    for (int p = 0; p < numberOfInsertsPerIteration; p++) {
                        JdbcTemplate t = getTemplate(client);
                        insertedCount += t.update(insertSql, new String[] {
                                ps.getExternalId() + "-" + i + "-" + p,
                                "The hyper blue dog jumped off a cliff" });
                    }                   
                }
                client.getEngine().push();
                try {
                    Thread.sleep(sleep);
                } catch (Exception ex) {
                }
            }
            done = true;
        }

    }

}
