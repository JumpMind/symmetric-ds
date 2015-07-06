/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.exception.InterruptedException;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIntegrationTest {

    final Logger logger = LoggerFactory.getLogger(getClass());

    private static ClientSymmetricEngine client;

    private static SymmetricWebServer server;

    private static String serverDatabase;

    private static String clientDatabase;

    protected static TestTablesService serverTestService;

    protected static TestTablesService clientTestService;

    protected ISymmetricEngine getClient() {
        if (client == null) {
            EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(new URL[] {
                    TestSetupUtil.getResource(DbTestUtils.DB_TEST_PROPERTIES),
                    TestSetupUtil.getResource("/symmetric-test.properties") }, "test.client",
                    new String[] { "client" });

            clientDatabase = properties.getProperty("test.client");
            client = new ClientSymmetricEngine(properties);
            client.getStagingManager().clean(0);
            clientTestService = new TestTablesService(client);
            TestSetupUtil
                    .dropAndCreateDatabaseTables(properties.getProperty("test.client"), client);
        }
        return client;
    }

    protected ISymmetricEngine getServer() {
        SymmetricWebServer webServer = getWebServer();
        return webServer != null ? webServer.getEngine() : null;
    }

    protected SymmetricWebServer getWebServer() {
        try {
            if (server == null) {
                EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(
                        new URL[] { TestSetupUtil.getResource(DbTestUtils.DB_TEST_PROPERTIES),
                                TestSetupUtil.getResource("/symmetric-test.properties") },
                        "test.root", new String[] { "root" });
                properties.setProperty(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT,
                        "/test-integration-root-setup.sql");
                serverDatabase = properties.getProperty("test.root");

                File rootDir = new File("target/root");
                FileUtils.deleteDirectory(rootDir);
                rootDir.mkdirs();
                File rootdbs = new File("target/rootdbs");
                FileUtils.deleteDirectory(rootdbs);
                rootdbs.mkdirs();

                File clientdbs = new File("target/clientdbs");
                FileUtils.deleteDirectory(clientdbs);
                clientdbs.mkdirs();

                File engineDir = new File(rootDir, "engines");
                engineDir.mkdirs();
                File rootPropertiesFile = new File(engineDir, "root.properties");
                FileOutputStream fos = new FileOutputStream(rootPropertiesFile);
                properties.store(fos, "unit tests");
                fos.close();

                ISymmetricEngine engine = TestSetupUtil.prepareRoot();
                engine.destroy();

                System.setProperty(SystemConstants.SYSPROP_ENGINES_DIR, engineDir.getAbsolutePath());
                System.setProperty(SystemConstants.SYSPROP_WEB_DIR, "src/main/deploy/web");
                SymmetricWebServer server = new SymmetricWebServer();
                server.setJmxEnabled(false);
                server.setJoin(false);
                server.start(Integer.parseInt(AppUtils.getPortNumber()));

                server.waitForEnginesToComeOnline(240000);

                serverTestService = new TestTablesService(server.getEngine());

                AbstractIntegrationTest.server = server;
            }
        } catch (Exception e) {
            logger.error("", e);
            fail(e.getMessage());
        }

        return server != null ? server : null;
    }

    protected boolean clientPush() {
        int tries = 0;
        boolean pushed = false;
        getClient().getRouterService().routeData(true);
        while (!pushed && tries < 10) {
            RemoteNodeStatuses statuses = getClient().push();
            statuses.waitForComplete(15000);
            pushed = statuses.wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pushed;
    }

    protected boolean clientPull() {
        int tries = 0;
        boolean pulled = false;
        while (!pulled && tries < 10) {
            RemoteNodeStatuses statuses = getClient().pull();
            try {
                statuses.waitForComplete(20000);
            } catch (InterruptedException ex) {
            }
            pulled = statuses.wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pulled;
    }

    protected void checkForFailedTriggers(boolean server, boolean client) {
        if (server) {
            ITriggerRouterService service = getServer().getTriggerRouterService();
            assertEquals(0, service.getFailedTriggers().size());
        }

        if (client) {
            ITriggerRouterService service = getClient().getTriggerRouterService();
            assertEquals(0, service.getFailedTriggers().size());
        }
    }

    protected Level setLoggingLevelForTest(Level level) {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.jumpmind");
        Level old = logger.getLevel();
        logger.setLevel(level);
        return old;
    }

    protected void logTestRunning() {
        logger.info("Running " + new Exception().getStackTrace()[1].getMethodName());
    }

    protected void logTestComplete() {
        logger.info("Completed running " + new Exception().getStackTrace()[1].getMethodName());
    }

    protected String printRootAndClientDatabases() {
        return String.format("{%s,%s}", serverDatabase, clientDatabase);
    }

    protected boolean isServerOracle() {
        return DatabaseNamesConstants.ORACLE.equals(getServer().getSymmetricDialect().getPlatform()
                .getName());
    }

    protected boolean isClientInterbase() {
        return DatabaseNamesConstants.INTERBASE.equals(getClient().getSymmetricDialect()
                .getPlatform().getName());
    }

    protected int getIncomingBatchCountForClient() {
        return getClient().getSqlTemplate().queryForInt("select count(*) from sym_incoming_batch");
    }

    protected int getIncomingBatchNotOkCountForClient() {
        return getClient().getSqlTemplate().queryForInt(
                "select count(*) from sym_incoming_batch where status != ?", Status.OK.name());
    }

    protected void assertNoPendingBatchesOnServer() {
        IOutgoingBatchService outgoingBatchService = getServer().getOutgoingBatchService();
        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(
                TestConstants.TEST_CLIENT_NODE.getNodeId(), false);
        assertEquals("There should be no outgoing batches", 0, batches.getBatches().size());
    }

    protected void assertNoPendingBatchesOnClient() {
        IOutgoingBatchService outgoingBatchService = getClient().getOutgoingBatchService();
        OutgoingBatches batches = outgoingBatchService.getOutgoingBatches(
                TestConstants.TEST_ROOT_NODE.getNodeId(), false);
        assertEquals("There should be no outgoing batches", 0, batches.getBatches().size());
    }

}
