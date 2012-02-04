package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.FileOutputStream;
import java.net.URL;

import junit.framework.Assert;

import org.apache.commons.io.FileUtils;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class AbstractIntegrationTest {

    private static final Logger logger = LoggerFactory.getLogger(AbstractIntegrationTest.class);

    private static ClientSymmetricEngine client;

    private static SymmetricWebServer server;

    protected ClientSymmetricEngine getClient() {
        if (client == null) {

        }
        return client;
    }

    protected SymmetricWebServer getServer() {
        try {
            if (server == null) {
                EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(
                        new URL[] { TestSetupUtil.getResource("/test-db.properties"),
                                TestSetupUtil.getResource("/symmetric-test.properties") },
                        "test.root", new String[] { "root" });
                properties.setProperty(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT, "/test-integration-root-setup.sql");


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
                server = new SymmetricWebServer();
                server.setJoin(false);
                server.start(51413);
                
                server.waitForEnginesToComeOnline(60000);
            }
        } catch (Exception e) {
            logger.error(e.getMessage(), e);
            Assert.fail(e.getMessage());
        }

        return server;
    }
}
