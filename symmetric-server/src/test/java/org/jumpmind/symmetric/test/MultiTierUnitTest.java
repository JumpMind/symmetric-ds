/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric.test;

import java.io.File;
import java.sql.Timestamp;
import java.util.Date;
import java.util.Map;

import javax.annotation.Resource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.RegistrationFailedException;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(locations = { "classpath:/multi-tier-test.xml" })
public class MultiTierUnitTest {

    static final Log logger = LogFactory.getLog(MultiTierUnitTest.class);

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
    protected SymmetricWebServer push001;

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
        INodeService nodeService = AppUtils.find(Constants.NODE_SERVICE, homeServer.getEngine());
        Node node = nodeService.findIdentity();
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getNodeId(), MultiTierTestConstants.NODE_ID_HOME);
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
        registerAndLoad(homeServer, MultiTierTestConstants.NODE_ID_REGION_1, region01Server,
                MultiTierTestConstants.NODE_GROUP_REGION, false, false, true);
    }

    @Test
    public void registerAndLoadRegion02() {
        registerAndLoad(homeServer, MultiTierTestConstants.NODE_ID_REGION_2, region02Server,
                MultiTierTestConstants.NODE_GROUP_REGION, false, false, true);
    }

    @Test
    public void registerAndLoadWorkstation000101DirectlyWithRegion01() {
        registerAndLoad(region01Server, MultiTierTestConstants.NODE_ID_STORE_0001_WORKSTATION_001, workstation000101,
                MultiTierTestConstants.NODE_GROUP_WORKSTATION, true, true, true);
    }

    /**
     * Test the registration redirect
     */
    @Test
    public void registerAndLoadWorkstation000102WithHomeServer() {
        IRegistrationService registrationService = AppUtils.find(Constants.REGISTRATION_SERVICE, homeServer.getEngine());
        registrationService.saveRegistrationRedirect(MultiTierTestConstants.NODE_ID_STORE_0001_WORKSTATION_002,
                MultiTierTestConstants.NODE_ID_REGION_1);
        workstation000102.getEngine().pull();
        IOutgoingBatchService outgoingBatchService = AppUtils.find(Constants.OUTGOING_BATCH_SERVICE, region01Server.getEngine());
        while (!outgoingBatchService.isInitialLoadComplete(MultiTierTestConstants.NODE_ID_STORE_0001_WORKSTATION_002)) {
            workstation000102.getEngine().pull();
        }
    }
    
    @Test
    public void testHeartbeatFromRegion01ToHomeServer() throws Exception {
        final String checkHeartbeatSql = "select heartbeat_time from sym_node where external_id='region01'";
        JdbcTemplate region01JdbcTemplate = (JdbcTemplate) region01Server.getEngine()
                .getApplicationContext().getBean(Constants.JDBC_TEMPLATE);
        JdbcTemplate homeJdbcTemplate = (JdbcTemplate) homeServer.getEngine()
                .getApplicationContext().getBean(Constants.JDBC_TEMPLATE);
        Date clientHeartbeatTimeBefore = region01JdbcTemplate.queryForObject(checkHeartbeatSql,
                Timestamp.class);
        Thread.sleep(1000);
        region01Server.getEngine().heartbeat(true);
        Date clientHeartbeatTimeAfter = region01JdbcTemplate.queryForObject(checkHeartbeatSql,
                Timestamp.class);
        Assert.assertNotSame("The heartbeat time was not updated at the client",
                clientHeartbeatTimeAfter, clientHeartbeatTimeBefore);
        Date rootHeartbeatTimeBefore = homeJdbcTemplate.queryForObject(checkHeartbeatSql,
                Timestamp.class);
        Assert
                .assertNotSame(
                        "The root heartbeat time should not be the same as the updated client heartbeat time",
                        clientHeartbeatTimeAfter, rootHeartbeatTimeBefore);
        while (region01Server.getEngine().push().wasDataProcessed()) {
            // continue to push while there data to push
        }
        Date rootHeartbeatTimeAfter = homeJdbcTemplate.queryForObject(checkHeartbeatSql,
                Timestamp.class);
        Assert.assertEquals(
                "The client heartbeat time should have been the same as the root heartbeat time.",
                clientHeartbeatTimeAfter, rootHeartbeatTimeAfter);
    }

    @Test
    public void sendDataFromHomeToAllWorkstations() {

    }

    @Test
    public void sendDataFromHomeToWorkstation000101Only() {

    }

    protected void registerAndLoad(SymmetricWebServer registrationServer, String externalId,
            SymmetricWebServer clientNode, String nodeGroupId, boolean autoRegister, boolean autoReload,
            boolean testReload) {
        if (!autoRegister) {
            registrationServer.getEngine().openRegistration(nodeGroupId, externalId);
        }
        clientNode.getEngine().pull();
        INodeService nodeService = AppUtils.find(Constants.NODE_SERVICE, clientNode.getEngine());
        Node node = nodeService.findIdentity();
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getNodeId(), externalId);

        if (testReload) {
            if (!autoReload) {
                registrationServer.getEngine().reloadNode(externalId);
            }

            IOutgoingBatchService homeOutgoingBatchService = AppUtils.find(Constants.OUTGOING_BATCH_SERVICE,
                    registrationServer.getEngine());
            while (!homeOutgoingBatchService.isInitialLoadComplete(externalId)) {
                clientNode.getEngine().pull();
            }

            NodeSecurity clientNodeSecurity = nodeService.findNodeSecurity(externalId);
            Assert.assertFalse(clientNodeSecurity.isInitialLoadEnabled());
            Assert.assertNotNull(clientNodeSecurity.getInitialLoadTime());
        }
    }

    /**
     * Test the registration process to a client that is only configured to push
     */
    @Test
    public void testRegistrationFromPushOnlyClient() {
        registerAndLoad(homeServer, "push", push001, "pushOnly", false, false, false);
    }

}