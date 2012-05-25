package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.IOException;
import java.net.URL;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.DbTestUtils;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

abstract public class TestSetupUtil {

    private static final Logger logger = LoggerFactory.getLogger(TestSetupUtil.class);

    static private ISymmetricEngine engine;

    public static ISymmetricEngine prepareForServiceTests() {
        if (engine == null) {
            engine = prepareRoot("/test-services-setup.sql");
            engine.start();
        }
        return engine;
    }

    protected static ISymmetricEngine prepareRoot() {
        return prepareRoot(null);
    }

    protected static ISymmetricEngine prepareRoot(String sql) {
        removeEmbededdedDatabases();
        EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(new URL[] {
                getResource(DbTestUtils.DB_TEST_PROPERTIES),
                getResource("/symmetric-test.properties") }, "test.root", new String[] { "root" });
        if (StringUtils.isNotBlank(sql)) {
            properties.setProperty(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT, sql);
        }
        ISymmetricEngine engine = new ClientSymmetricEngine(properties);
        dropAndCreateDatabaseTables(properties.getProperty("test.root"), engine);
        return engine;
    }

    public static IDatabasePlatform dropDatabaseTables(String databaseType, ISymmetricEngine engine) {
        
        ISymmetricDialect dialect = engine.getSymmetricDialect();

        final AbstractJdbcDatabasePlatform platform = (AbstractJdbcDatabasePlatform) dialect
                .getPlatform();
        
        dialect.cleanupTriggers();

        IDdlBuilder builder = platform.getDdlBuilder();

        String fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + "-pre.sql";
        URL url = getResource(fileName);
        if (url != null) {
            new SqlScript(url, dialect.getPlatform().getSqlTemplate(), false).execute(true);
        }

        Database db2drop = platform.readDatabase(platform.getDefaultCatalog(),
                platform.getDefaultSchema(), new String[] { "TABLE" });
        
        String sql = builder.dropTables(db2drop);                
        
        SqlScript dropScript = new SqlScript(sql, dialect.getPlatform().getSqlTemplate(), false);
        dropScript.execute(true);

        new SqlScript(getResource(TestConstants.TEST_DROP_ALL_SCRIPT), platform.getSqlTemplate(),
                false).execute(true);
        ((ParameterService) engine.getParameterService()).setDatabaseHasBeenInitialized(false);

        fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + ".sql";
        url = getResource(fileName);
        if (url != null) {
            new SqlScript(url, dialect.getPlatform().getSqlTemplate(), false).execute(true);
        }

        dialect.purge();

        return platform;
    }

    public static void dropAndCreateDatabaseTables(String databaseType, ISymmetricEngine engine) {
        IDatabasePlatform platform = dropDatabaseTables(databaseType, engine);
        Database testDb = platform.readDatabaseFromXml("/test-schema.xml", true);
        platform.createDatabase(testDb, false, true);
    }

    protected static void removeEmbededdedDatabases() {
        File clientDbDir = new File("target/clientdbs");
        if (clientDbDir.exists()) {
            try {
                logger.info("Removing client database files");
                FileUtils.deleteDirectory(clientDbDir);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
        File rootDbDir = new File("target/rootdbs");
        if (rootDbDir.exists()) {
            try {
                logger.info("Removing root database files");
                FileUtils.deleteDirectory(rootDbDir);
            } catch (IOException e) {
                logger.error(e.getMessage(), e);
            }
        }
    }

    protected static URL getResource(String resource) {
        return TestSetupUtil.class.getResource(resource);
    }

}
