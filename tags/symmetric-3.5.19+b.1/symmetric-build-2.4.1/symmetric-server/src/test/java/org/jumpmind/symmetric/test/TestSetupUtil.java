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
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.servlet.ServletContext;
import javax.sql.DataSource;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.StandaloneSymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.io.DatabaseIO;
import org.jumpmind.symmetric.ddl.model.Database;
import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;

/**
 * 
 */
public class TestSetupUtil {

    private static final String CLIENT = ".client";

    private static final String ROOT = ".root";

    static final Log logger = LogFactory.getLog(TestSetupUtil.class);

    private static ISymmetricEngine clientEngine;

    private static SymmetricWebServer rootServer;

    public static final int TEST_PORT = 51413;

    public static Collection<String[]> lookupDatabasePairs(String testPrefix) {
        Properties properties = getTestProperties(testPrefix);
        String[] clientDatabaseTypes = StringUtils.split(properties.getProperty(testPrefix + CLIENT), ",");
        String[] rootDatabaseTypes = getRootDbTypes(testPrefix);

        String[][] clientAndRootCombos = new String[rootDatabaseTypes.length * clientDatabaseTypes.length][2];

        int index = 0;
        for (String rootDatabaseType : rootDatabaseTypes) {
            for (String clientDatabaseType : clientDatabaseTypes) {
                clientAndRootCombos[index][0] = clientDatabaseType;
                clientAndRootCombos[index++][1] = rootDatabaseType;
            }
        }
        return Arrays.asList(clientAndRootCombos);
    }

    public static Collection<String[]> lookupDatabases(String testPrefix) {
        List<String[]> list = new ArrayList<String[]>();
        String[] dbs = getRootDbTypes(testPrefix);
        for (String string : dbs) {
            list.add(new String[] { string });
        }
        return list;
    }

    public static void cleanup() throws Exception {
        if (clientEngine != null) {
            clientEngine.destroy();
            clientEngine = null;
        }
        if (rootServer != null) {
            rootServer.stop();
            rootServer = null;
        }

        closeDerbyAndReloadDriver();
    }

    /**
     * Unit tests were failing after opening and closing several data connection
     * pools against the same database in memory. After shutting down the
     * connection pool, it seems to help by shutting down the entire database.
     */
    protected static void closeDerbyAndReloadDriver() throws SQLException {
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 50000) {
                DriverManager.registerDriver(new EmbeddedDriver());
            }
        } catch (Exception ex) {
            // derby not in use ...
        }
    }

    public static void setup(String testPrefix, String sqlScriptSuffix, String clientDb, String rootDb)
            throws Exception {

        removeEmbededdedDatabases();

        if (rootDb != null) {
            // Temporary engine used for test database setup
            ISymmetricEngine setupEngine = new StandaloneSymmetricEngine("file:"
                    + writeTempPropertiesFileFor(testPrefix, rootDb, DatabaseRole.ROOT).getAbsolutePath(), null);            
            dropAndCreateDatabaseTables(rootDb, setupEngine);
            setupEngine.setup();
            
            DataSource ds = (DataSource) setupEngine.getApplicationContext().getBean(Constants.DATA_SOURCE);
            IDbDialect dialect = (IDbDialect) setupEngine.getApplicationContext().getBean(Constants.DB_DIALECT);
            new SqlScript(getResource("/" + testPrefix + sqlScriptSuffix), ds, true, SqlScript.QUERY_ENDS, dialect.getSqlScriptReplacementTokens()).execute();
            setupEngine.destroy();
            
            rootServer = new SymmetricWebServer("file:"
                    + writeTempPropertiesFileFor(testPrefix, rootDb, DatabaseRole.ROOT).getAbsolutePath(), "src/main/deploy/web");
            rootServer.setJoin(false);
            rootServer.start(TEST_PORT);
            
        }

        if (clientDb != null) {
            String file = writeTempPropertiesFileFor(testPrefix, clientDb, DatabaseRole.CLIENT).getAbsolutePath();
            Properties properties = new Properties();
            FileReader reader = new FileReader(file);
            properties.load(reader);
            IOUtils.closeQuietly(reader);
            clientEngine = new StandaloneSymmetricEngine(properties);
            dropAndCreateDatabaseTables(clientDb, clientEngine);
        }
    }

    public static void removeEmbededdedDatabases() {
        File derby = new File("target/derby");
        if (derby.exists()) {
            try {
                logger.info("Removing derby database files.");
                FileUtils.deleteDirectory(derby);
            } catch (IOException e) {
                logger.error(e, e);
            }
        }
        File h2 = new File("target/h2");
        if (h2.exists()) {
            try {
                logger.info("Removing h2 database files");
                FileUtils.deleteDirectory(h2);
            } catch (IOException e) {
                logger.error(e, e);
            }
        }
        File hsqldb = new File("target/hsqldb");
        if (hsqldb.exists()) {
            try {
                logger.info("Removing hsqldb database files");
                FileUtils.deleteDirectory(hsqldb);
            } catch (IOException e) {
                logger.error(e, e);
            }
        }
        File sqlitedb = new File("target/sqlite");
        if (sqlitedb.exists() && FileUtils.listFiles(sqlitedb, null, true).size() > 0) {
            try {
                logger.info("Removing sqlite database files");
                FileUtils.deleteDirectory(sqlitedb);

            } catch (IOException e) {
                logger.error(e, e);
            }
        }
        sqlitedb.mkdirs();
    }

    public static ISymmetricEngine getClientEngine() {
        return clientEngine;
    }
    
    public static SymmetricWebServer getRootServer() {
        return rootServer;
    }   
    
    public static ISymmetricEngine getRootEngine() {
        return rootServer.getEngine();
    }

    public static boolean isConnectionValid(Properties properties) throws Exception {
        try {
            Class.forName(properties.getProperty("db.driver"));
            Connection c = DriverManager.getConnection(properties.getProperty("db.url"), properties
                    .getProperty("db.user"), properties.getProperty("db.password"));
            c.close();
            return true;
        } catch (Exception ex) {
            logger.error("Could not connect to the test database using the url: " + properties.getProperty("db.url")
                    + " and classpath: " + System.getProperty("java.class.path"), ex);
            return false;
        }
    }

    protected static void dropAndCreateDatabaseTables(String databaseType, ISymmetricEngine engine) {
        DataSource ds = (DataSource) engine.getApplicationContext().getBean(Constants.DATA_SOURCE);
        try {
            IDbDialect dialect = (IDbDialect) engine.getApplicationContext().getBean(Constants.DB_DIALECT);
            Platform platform = dialect.getPlatform();
            
            dialect.cleanupTriggers();

            String fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + "-pre.sql";
            URL url = getResource(fileName);
            if (url != null) {
                new SqlScript(url, ds, false).execute(true);
            }            
                        
            Database testDb = getTestDatabase();
            new SqlScript(platform.getDropTablesSql(testDb, true), ds, false).execute(true);            

            new SqlScript(getResource(TestConstants.TEST_DROP_ALL_SCRIPT), ds, false).execute(true);

            fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + ".sql";
            url = getResource(fileName);
            if (url != null) {
                new SqlScript(url, ds, false).execute(true);
            }

            dialect.purge();
            
            platform.createTables(testDb, false, true);
            platform.createTables(getPreviousSymmetricVersionTables(), false, true);

        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected static URL getResource(String resource) {
        return TestSetupUtil.class.getResource(resource);
    }

    protected static Database getTestDatabase() throws IOException {
        return new DatabaseIO().read(new InputStreamReader(getResource("/test-tables-ddl.xml").openStream()));
    }

    protected static Database getPreviousSymmetricVersionTables() throws IOException {
        return new DatabaseIO().read(new InputStreamReader(getResource("/test-tables-old-symmetric-ddl.xml")
                .openStream()));
    }

    public static File writeTempPropertiesFileFor(String testPrefix, String databaseType, DatabaseRole databaseRole) {
        try {
            Properties testProperties = getTestProperties(testPrefix);
            Properties newProperties = new Properties();
            Set<Object> keys = testProperties.keySet();
            for (Object string : keys) {
                String key = (String) string;
                String dbRoleReplaceToken = databaseType + "." + databaseRole.name().toLowerCase() + ".";
                if (key.startsWith(dbRoleReplaceToken)) {
                    String newKey = key.substring(dbRoleReplaceToken.length());
                    newProperties.put(newKey, testProperties.get(key));
                } else if (key.startsWith(databaseType)) {
                    String newKey = key.substring(databaseType.length() + 1);
                    newProperties.put(newKey, testProperties.get(key));
                } else {
                    newProperties.put(key, testProperties.get(key));
                }
            }
            
            String dbDriver = newProperties.getProperty("db.driver");
            Assert.assertNotNull("Could not find a driver for '" + databaseType + "'", dbDriver);

            if (isConnectionValid(newProperties)) {
                newProperties.setProperty(ParameterConstants.NODE_GROUP_ID,
                        databaseRole == DatabaseRole.CLIENT ? TestConstants.TEST_CLIENT_NODE_GROUP
                                : TestConstants.TEST_ROOT_NODE_GROUP);
                newProperties.setProperty(ParameterConstants.EXTERNAL_ID,
                        databaseRole == DatabaseRole.ROOT ? TestConstants.TEST_ROOT_EXTERNAL_ID
                                : TestConstants.TEST_CLIENT_EXTERNAL_ID);
                newProperties.setProperty(ParameterConstants.SYNC_URL, databaseRole == DatabaseRole.CLIENT ? "" : ("http://localhost:" + TEST_PORT + "/sync"));
                newProperties.setProperty(ParameterConstants.REGISTRATION_URL,
                        databaseRole == DatabaseRole.CLIENT ? "http://localhost:" + TEST_PORT + "/sync" : "");
                newProperties.setProperty(ParameterConstants.ENGINE_NAME, databaseRole.getName());

                File propertiesFile = File.createTempFile("symmetric-test.", ".properties");
                FileOutputStream os = new FileOutputStream(propertiesFile);
                newProperties.store(os, "generated by the symmetricds unit tests");
                os.close();
                propertiesFile.deleteOnExit();
                return propertiesFile;

            } else {
                Assert.fail("Could not find a valid connection for " + databaseType);
                return null;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    public static Properties getTestProperties(String testPrefix) {
        try {
            final String TEST_PROPERTIES_FILE = "/symmetric-" + testPrefix + ".properties";
            Properties properties = new Properties();

            properties.load(TestSetupUtil.class.getResourceAsStream(TEST_PROPERTIES_FILE));
            String homeDir = System.getProperty("user.home");
            File propertiesFile = new File(homeDir + TEST_PROPERTIES_FILE);
            if (propertiesFile.exists()) {
                FileInputStream f = new FileInputStream(propertiesFile);
                properties.load(f);
                f.close();
            } else {
                if (logger.isDebugEnabled()) {
                    logger.debug("Could not find " + propertiesFile.getAbsolutePath()
                            + ". Using all of the default properties");
                }
            }

            String rootDbs = System.getProperty(testPrefix + ROOT);
            String clientDbs = System.getProperty(testPrefix + CLIENT);
            if (!StringUtils.isBlank(rootDbs) && !rootDbs.startsWith("${")) {
                properties.setProperty(testPrefix + ROOT, rootDbs);
            }
            if (!StringUtils.isBlank(clientDbs) && !clientDbs.startsWith("${")) {
                properties.setProperty(testPrefix + CLIENT, clientDbs);
            }
            return properties;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected static String[] getRootDbTypes(String testPrefix) {
        Properties properties = getTestProperties(testPrefix);
        return StringUtils.split(properties.getProperty(testPrefix + ROOT), ",");
    }

    public static MockHttpServletRequest createMockHttpServletRequest(ServletContext servletContext, String method,
            String uri, Map<String, String> parameters) {
        final String[] uriParts = StringUtils.split(uri, "?");
        final MockHttpServletRequest request = new MockHttpServletRequest(servletContext, method, uriParts[0]);
        if (uriParts.length > 1) {
            request.setQueryString(uriParts[1]);
        }
        if (parameters != null) {
            request.setParameters(parameters);
        }
        return request;
    }

}