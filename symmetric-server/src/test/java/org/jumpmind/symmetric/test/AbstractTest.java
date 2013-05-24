package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.exception.InterruptedException;
import org.jumpmind.exception.IoException;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.SystemConstants;
import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.IncomingBatch.Status;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.util.AppUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class AbstractTest {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    private Map<String, SymmetricWebServer> webServers = new HashMap<String, SymmetricWebServer>();

    private static final int REGISTRATION_PORT = 9999;

    private int port = REGISTRATION_PORT;

    /**
     * The registration server should always be the first group in the list
     */
    protected String[] getGroupNames() {
        return new String[] { "root", "client" };
    }

    protected SymmetricWebServer getRegServer() {
        return getWebServer(getGroupNames()[0]);
    }

    protected Table[] getTables(String name) {
        return null;
    }

    protected Properties getProperties(String name) {
        TypedProperties properties = new TypedProperties(getClass().getResourceAsStream(
                "/symmetric-test.properties"));
        properties.setProperty(ParameterConstants.ENGINE_NAME, name);
        properties.setProperty(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, "true");
        properties.setProperty(ParameterConstants.EXTERNAL_ID, name);
        properties.setProperty(ParameterConstants.NODE_GROUP_ID, name);
        properties.setProperty(ParameterConstants.SYNC_URL, "http://localhost:" + port + "/sync/"
                + name);
        properties.setProperty(ParameterConstants.REGISTRATION_URL, "http://localhost:"
                + REGISTRATION_PORT + "/sync/" + getGroupNames()[0]);
        return properties;
    }

    @Before
    public void setup() {
        TestSetupUtil.removeEmbededdedDatabases();
        String[] groups = getGroupNames();
        for (String group : groups) {
            getWebServer(group);
        }
    }

    @Test
    public void test() throws Exception {
        ISymmetricEngine rootServer = getRegServer().getEngine();
        ISymmetricEngine clientServer = getWebServer("client").getEngine();
        test(rootServer, clientServer);
    }

    protected abstract void test(ISymmetricEngine rootServer, ISymmetricEngine clientServer) throws Exception;
    
    @After
    public void teardown() {
        String[] groups = getGroupNames();
        for (String group : groups) {
            SymmetricWebServer webServer = getWebServer(group);
            if (webServer != null) {
                try {
                    webServer.stop();
                } catch (Exception e) {
                    log.error(e.getMessage(), e);
                }
            }
        }
    }

    protected SymmetricWebServer getWebServer(String name) {
        try {
            if (!webServers.containsKey(name)) {

                EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(
                        new URL[] { getResource(DbTestUtils.DB_TEST_PROPERTIES) }, "test." + name,
                        new String[] { name });
                properties.putAll(getProperties(name));
                File rootDir = new File("target/" + name);
                FileUtils.deleteDirectory(rootDir);
                rootDir.mkdirs();

                File engineDir = new File(rootDir, "engines");
                engineDir.mkdirs();

                File rootPropertiesFile = new File(engineDir, "root.properties");
                FileOutputStream fos = new FileOutputStream(rootPropertiesFile);
                properties.store(fos, "unit tests");
                fos.close();

                System.setProperty(SystemConstants.SYSPROP_WAIT_FOR_DATABASE, "false");
                System.setProperty(SystemConstants.SYSPROP_ENGINES_DIR, engineDir.getAbsolutePath());
                System.setProperty(SystemConstants.SYSPROP_WEB_DIR, "src/main/deploy/web");

                ISymmetricEngine engine = new ClientSymmetricEngine(properties);
                IDatabasePlatform platform = engine.getDatabasePlatform();
                engine.uninstall();

                Database database = platform.getDdlReader().readTables(
                        platform.getDefaultCatalog(), platform.getDefaultSchema(),
                        new String[] { "TABLE" });
                platform.dropDatabase(database, true);

                Table[] tables = getTables(name);
                if (tables != null) {
                    platform.alterCaseToMatchDatabaseDefaultCase(tables);
                    platform.createTables(false, true, tables);
                }
                engine.destroy();

                SymmetricWebServer server = new SymmetricWebServer();
                server.setJmxEnabled(false);
                server.setHttpPort(port++);
                server.setJoin(false);
                server.start();

                server.waitForEnginesToComeOnline(60000);

                webServers.put(name, server);

            }
            return webServers.get(name);

        } catch (IOException e) {
            throw new IoException(e);
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected URL getResource(String resource) {
        return AbstractTest.class.getResource(resource);
    }

    /**
     * Loads configuration in the format of classname.csv at the registration server
     */
    protected void loadConfigAtRegistrationServer() throws Exception {
        ISymmetricEngine regEngine = getRegServer().getEngine();
        IDataLoaderService dataLoaderService = regEngine.getDataLoaderService();
        boolean inError = false;
        InputStream is = getClass().getResourceAsStream(getClass().getSimpleName() + ".csv");
        Assert.assertNotNull("Could not find configuration as a resource", is);
        List<IncomingBatch> batches = dataLoaderService.loadDataBatch(IOUtils.toString(is));
        for (IncomingBatch batch : batches) {
            if (batch.getStatus() == Status.ER) {
                inError = true;
            }
        }

        Assert.assertFalse("Failed to load configuration", inError);

    }
    
    protected boolean pull(String name) {
        int tries = 0;
        boolean pulled = false;
        while (!pulled && tries < 10) {
            RemoteNodeStatuses statuses = getWebServer(name).getEngine().pull();
            try {
                statuses.waitForComplete(60000);
            } catch (InterruptedException ex) {
                log.warn(ex.getMessage());
            }
            pulled = statuses.wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pulled;
    }
    
    protected boolean pullFiles(String name) {
        int tries = 0;
        boolean pulled = false;
        while (!pulled && tries < 10) {
            RemoteNodeStatuses statuses = getWebServer(name).getEngine().getFileSyncService().pullFilesFromNodes(true);
            try {
                statuses.waitForComplete(60000);
            } catch (InterruptedException ex) {
                log.warn(ex.getMessage());
            }
            pulled = statuses.wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pulled;
    }  
    
    protected boolean pushFiles(String name) {
        int tries = 0;
        boolean pulled = false;
        while (!pulled && tries < 10) {
            RemoteNodeStatuses statuses = getWebServer(name).getEngine().getFileSyncService().pushFilesToNodes(true);
            try {
                statuses.waitForComplete(60000);
            } catch (InterruptedException ex) {
                log.warn(ex.getMessage());
            }
            pulled = statuses.wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pulled;
    }    
        
    
    protected void loadConfigAndRegisterNode(String clientGroup, String serverGroup) throws Exception {
        loadConfigAtRegistrationServer();
        getWebServer(serverGroup).getEngine().getFileSyncService().trackChanges(true);
        getWebServer(serverGroup).getEngine().route();
        getWebServer(serverGroup).getEngine().openRegistration(clientGroup, clientGroup);
        pull(clientGroup);
    }

}
