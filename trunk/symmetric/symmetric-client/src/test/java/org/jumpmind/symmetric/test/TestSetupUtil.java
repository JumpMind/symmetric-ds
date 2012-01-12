package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.Properties;

import org.apache.commons.io.FileUtils;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.properties.EnvironmentSpecificProperties;
import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.TestConstants;
import org.jumpmind.symmetric.TestUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class TestSetupUtil {

    static private ISymmetricEngine engine;

    public static ISymmetricEngine prepareForServiceTests() {
        if (engine == null) {
            removeEmbededdedDatabases();
            EnvironmentSpecificProperties properties = new EnvironmentSpecificProperties(
                    "test.root", new String[] { "root" });
            properties.setProperty(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT,
                    "/test-services-setup.sql");
            engine = new ClientSymmetricEngine(properties);
            dropAndCreateDatabaseTables(properties.getProperty("test.root"), engine);
            engine.start();
        }
        return engine;
    }

    protected static void dropAndCreateDatabaseTables(String databaseType, ISymmetricEngine engine) {
        try {
            ISymmetricDialect dialect = engine.getSymmetricDialect();
            IDatabasePlatform platform = dialect.getPlatform();

            dialect.cleanupTriggers();

            String fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + "-pre.sql";
            URL url = getResource(fileName);
            if (url != null) {
                new SqlScript(url, dialect.getPlatform().getSqlTemplate(), false).execute(true);
            }

            Database testDb = getTestDatabase();
            IDdlBuilder builder = platform.getDdlBuilder();
            String sql = builder.dropTables(testDb);
            new SqlScript(sql, dialect.getPlatform().getSqlTemplate(), false).execute(true);

            new SqlScript(getResource(TestConstants.TEST_DROP_ALL_SCRIPT),
                    platform.getSqlTemplate(), false).execute(true);

            fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + ".sql";
            url = getResource(fileName);
            if (url != null) {
                new SqlScript(url, dialect.getPlatform().getSqlTemplate(), false).execute(true);
            }

            dialect.purge();

            platform.createDatabase(testDb, false, true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static Database getTestDatabase() throws IOException {
        return new DatabaseIO().read(new InputStreamReader(getResource("/test-schema.xml")
                .openStream()));
    }

    protected static boolean isConnectionValid(Properties properties) throws Exception {
        try {
            Class.forName(properties.getProperty("db.driver"));
            Connection c = DriverManager.getConnection(properties.getProperty("db.url"),
                    properties.getProperty("db.user"), properties.getProperty("db.password"));
            c.close();
            return true;
        } catch (Exception ex) {
            TestUtils.getLog().error(
                    "Could not connect to the test database using the url: "
                            + properties.getProperty("db.url") + " and classpath: "
                            + System.getProperty("java.class.path"), ex);
            return false;
        }
    }

    protected static void removeEmbededdedDatabases() {
        File derby = new File("target/derby");
        if (derby.exists()) {
            try {
                TestUtils.getLog().info("Removing derby database files.");
                FileUtils.deleteDirectory(derby);
            } catch (IOException e) {
                TestUtils.getLog().error(e);
            }
        }
        File h2 = new File("target/h2");
        if (h2.exists()) {
            try {
                TestUtils.getLog().info("Removing h2 database files");
                FileUtils.deleteDirectory(h2);
            } catch (IOException e) {
                TestUtils.getLog().error(e);
            }
        }
        File hsqldb = new File("target/hsqldb");
        if (hsqldb.exists()) {
            try {
                TestUtils.getLog().info("Removing hsqldb database files");
                FileUtils.deleteDirectory(hsqldb);
            } catch (IOException e) {
                TestUtils.getLog().error(e);
            }
        }
        File sqlitedb = new File("target/sqlite");
        if (sqlitedb.exists() && FileUtils.listFiles(sqlitedb, null, true).size() > 0) {
            try {
                TestUtils.getLog().info("Removing sqlite database files");
                FileUtils.deleteDirectory(sqlitedb);

            } catch (IOException e) {
                TestUtils.getLog().error(e);
            }
        }
        sqlitedb.mkdirs();
    }

    protected static URL getResource(String resource) {
        return TestSetupUtil.class.getResource(resource);
    }

}
