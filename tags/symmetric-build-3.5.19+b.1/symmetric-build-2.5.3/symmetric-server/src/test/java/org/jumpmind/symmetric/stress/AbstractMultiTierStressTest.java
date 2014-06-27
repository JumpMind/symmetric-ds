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
package org.jumpmind.symmetric.stress;

import java.util.Map;

import javax.annotation.Resource;

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
import org.jumpmind.symmetric.test.MultiTierTestConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
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

    @Test(timeout = 120000)
    public void validateHomeServerStartup() {
        INodeService nodeService = AppUtils.find(Constants.NODE_SERVICE,
                homeServer.getEngine());
        Node node = nodeService.findIdentity();
        Assert.assertNotNull(node);
        Assert.assertEquals(node.getNodeId(),
                MultiTierTestConstants.NODE_ID_HOME);
    }

    @Test(timeout = 120000)
    public void registerAndLoadRegion() {
        registerAndLoad(homeServer, MultiTierTestConstants.NODE_ID_REGION_1,
                regionServer, MultiTierTestConstants.NODE_GROUP_REGION, false,
                false, true);
    }

    @Test(timeout = 120000)
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
    public void pushTest() {
        getTemplate(workstation000101).update(
                "truncate table sync_workstation_to_home");
        getTemplate(workstation000102).update(
                "truncate table sync_workstation_to_home");
        getTemplate(regionServer).update(
                "truncate table sync_workstation_to_home");
        getTemplate(homeServer).update(
                "truncate table sync_workstation_to_home");

        PushThread w1 = new PushThread(unitTestSql
                .get("insertWorkstationToHomeSql"), workstation000101, 250, 500,
                10);
        PushThread w2 = new PushThread(unitTestSql
                .get("insertWorkstationToHomeSql"), workstation000102, 500, 250,
                12);
        w2.start();
        w1.start();
        while (regionServer.getEngine().push().wasDataProcessed() || !w2.done || !w1.done) {
            try {
                Thread.sleep(5);
            } catch (Exception ex) {
            }
        }

        JdbcTemplate t = getTemplate(homeServer);
        int countAtHomeServer = t
                .queryForInt("select count(*) from sync_workstation_to_home");
        int countInserted = w1.insertedCount + w2.insertedCount;
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
                clientNode.getEngine());
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
                    Constants.OUTGOING_BATCH_SERVICE, registrationServer.getEngine());
            while (!homeOutgoingBatchService.areAllLoadBatchesComplete(externalId)) {
                clientNode.getEngine().pull();
            }

            NodeSecurity clientNodeSecurity = nodeService
                    .findNodeSecurity(externalId);
            Assert.assertFalse(clientNodeSecurity.isInitialLoadEnabled());
            Assert.assertNotNull(clientNodeSecurity.getInitialLoadTime());
        }
    }

    protected JdbcTemplate getTemplate(SymmetricWebServer server) {
        JdbcTemplate t = AppUtils.find(Constants.JDBC_TEMPLATE, server.getEngine());
        return t;
    }

    protected IParameterService getParameterService(SymmetricWebServer server) {
        IParameterService s = AppUtils
                .find(Constants.PARAMETER_SERVICE, server.getEngine());
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
            this.setName("stressthread"
                    + getParameterService(client).getExternalId());
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
                        insertedCount += t.update(insertSql,
                                ps.getExternalId() + "-" + i + "-" + p,
                                "The hyper blue dog jumped off a cliff" );
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