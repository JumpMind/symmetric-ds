package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.apache.log4j.Level;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    private static ClientSymmetricEngine client;

    private static SymmetricWebServer server;
    
    private static String serverDatabase;
    
    private static String clientDatabase;
        
    protected static TestTablesService serverTestService;
    
    protected static TestTablesService clientTestService;

    protected ClientSymmetricEngine getClient() {
        if (client == null) {
            EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(new URL[] {
                    TestSetupUtil.getResource(DbTestUtils.DB_TEST_PROPERTIES),
                    TestSetupUtil.getResource("/symmetric-test.properties") }, "test.client",
                    new String[] { "client" });

            clientDatabase = properties.getProperty("test.client");
            client = new ClientSymmetricEngine(properties);
            clientTestService = new TestTablesService(client);
            TestSetupUtil.dropAndCreateDatabaseTables(properties.getProperty("test.client"), client);
        }
        return client;
    }

    protected SymmetricWebServer getServer() {
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
                File engineDir = new File(rootDir, "engines");
                engineDir.mkdirs();
                File rootPropertiesFile = new File(engineDir, "root.properties");
                FileOutputStream fos = new FileOutputStream(rootPropertiesFile);
                properties.store(fos, "unit tests");
                fos.close();

                ISymmetricEngine engine = TestSetupUtil.prepareRoot();
                engine.destroy();

                System.setProperty(Constants.SYS_PROP_ENGINES_DIR, engineDir.getAbsolutePath());
                System.setProperty(Constants.SYS_PROP_WEB_DIR, "src/main/deploy/web");
                SymmetricWebServer server = new SymmetricWebServer();                
                server.setJoin(false);
                server.start(51413);
                
                server.waitForEnginesToComeOnline(60000);
                
                
                serverTestService = new TestTablesService(server.getEngine());
                
                AbstractIntegrationTest.server = server;
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
        }

        return server;
    }

    protected boolean clientPush() {
        int tries = 0;
        boolean pushed = false;
        while (!pushed && tries < 10) {
            pushed = getClient().push().wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pushed;
    }

    protected boolean clientPull() {
        int tries = 0;
        boolean pulled = false;
        while (!pulled && tries < 10) {
            pulled = getClient().pull().wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pulled;
    }
    
    protected void checkForFailedTriggers(boolean server, boolean client) {
        if (server) {
            ITriggerRouterService service = getServer().getEngine().getTriggerRouterService();
            Assert.assertEquals(0, service.getFailedTriggers().size());
        }

        if (client) {
            ITriggerRouterService service = getClient().getTriggerRouterService();
            Assert.assertEquals(0, service.getFailedTriggers().size());
        }
    }    

    protected Level setLoggingLevelForTest(Level level) {
        org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger("org.jumpmind");
        Level old = logger.getLevel();
        logger.setLevel(level);
        return old;
    }

    protected void logTestRunning() {
        logger.info("Running " + new Exception().getStackTrace()[1].getMethodName() + ". ");
    }

    protected void logTestComplete() {
        logger.info("Completed running " + new Exception().getStackTrace()[1].getMethodName());
    }
    
    protected String printRootAndClientDatabases() {
        return String.format("{%s,%s}", serverDatabase, clientDatabase);
    }

}
