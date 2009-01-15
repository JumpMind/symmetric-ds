/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */
package org.jumpmind.symmetric.test;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
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

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.apache.derby.jdbc.EmbeddedDriver;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.SymmetricWebServer;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.springframework.mock.web.MockHttpServletRequest;

public class TestSetupUtil {

    static final Log logger = LogFactory.getLog(TestSetupUtil.class);

    static private SymmetricEngine clientEngine;

    static private SymmetricWebServer rootServer;

    public static Collection<String[]> lookupDatabasePairs(String testPrefix) {
        Properties properties = getTestProperties(testPrefix);
        String[] clientDatabaseTypes = StringUtils.split(properties.getProperty(testPrefix + ".client"), ",");
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
            clientEngine.stop();
            clientEngine = null;
        }
        if (rootServer != null) {
            rootServer.stop();
            rootServer = null;
        }

        closeDerbyAndReloadDriver();
    }
    
    /**
     * Unit tests were failing after opening and closing several data connection pools against the 
     * same database in memory.  After shutting down the connection pool, it seems to help by shutting
     * down the entire database.
     */
    protected static void closeDerbyAndReloadDriver() throws SQLException {
        try {
            DriverManager.getConnection("jdbc:derby:;shutdown=true");            
        } catch (SQLException ex) {
            if (ex.getErrorCode() == 50000) {
                DriverManager.registerDriver(new EmbeddedDriver());
                logger.info(ex.getMessage());
            }
        } catch (Exception ex) {
            // derby not in use ...
        }
    }

    public static void setup(String testPrefix, String sqlScriptSuffix, String clientDb, String rootDb)
            throws Exception {
        if (rootDb != null) {
            rootServer = new SymmetricWebServer(new SymmetricEngine("file:"
                    + writeTempPropertiesFileFor(testPrefix, rootDb, DatabaseRole.ROOT).getAbsolutePath()));
            dropAndCreateDatabaseTables(rootDb, rootServer.getEngine());
            IBootstrapService bootstrapService = AppUtils.find(Constants.BOOTSTRAP_SERVICE, rootServer.getEngine());
            bootstrapService.setupDatabase();
            new SqlScript(getResource("/" + testPrefix + sqlScriptSuffix), (DataSource) rootServer.getEngine()
                    .getApplicationContext().getBean(Constants.DATA_SOURCE), true).execute();
            rootServer.setJoin(false);
            rootServer.start(8888);
        }

        if (clientDb != null) {
            clientEngine = new SymmetricEngine("file:"
                    + writeTempPropertiesFileFor(testPrefix, clientDb, DatabaseRole.CLIENT).getAbsolutePath(), null);
            dropAndCreateDatabaseTables(clientDb, clientEngine);
        }
    }

    public static SymmetricEngine getRootEngine() {
        return rootServer.getEngine();
    }

    public static SymmetricEngine getClientEngine() {
        return clientEngine;
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

    protected static void dropAndCreateDatabaseTables(String databaseType, SymmetricEngine engine) {
        DataSource ds = (DataSource) engine.getApplicationContext().getBean(Constants.DATA_SOURCE);
        try {
            IDbDialect dialect = (IDbDialect) engine.getApplicationContext().getBean(Constants.DB_DIALECT);
            Platform platform = dialect.getPlatform();
            Database testDb = getTestDatabase();
            platform.dropTables(testDb, true);
            dialect.purge();

            new SqlScript(getResource(TestConstants.TEST_DROP_ALL_SCRIPT), ds, false).execute();

            String fileName = TestConstants.TEST_DROP_SEQ_SCRIPT + databaseType + ".sql";
            URL url = getResource(fileName);
            if (url != null) {
                new SqlScript(url, ds, false).execute();
            }

            platform.createTables(testDb, false, true);

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

    public static File writeTempPropertiesFileFor(String testPrefix, String databaseType, DatabaseRole databaseRole) {
        try {
            Properties properties = getTestProperties(testPrefix);
            Properties newProperties = new Properties();
            Set<Object> keys = properties.keySet();
            for (Object string : keys) {
                String key = (String) string;
                String dbRoleReplaceToken = databaseType + "." + databaseRole.name().toLowerCase() + ".";
                if (key.startsWith(dbRoleReplaceToken)) {
                    String newKey = key.substring(dbRoleReplaceToken.length());
                    newProperties.put(newKey, properties.get(key));
                } else if (key.startsWith(databaseType)) {
                    String newKey = key.substring(databaseType.length() + 1);
                    newProperties.put(newKey, properties.get(key));
                } else {
                    newProperties.put(key, properties.get(key));
                }
            }

            if (isConnectionValid(newProperties)) {
                newProperties.setProperty(ParameterConstants.NODE_GROUP_ID,
                        databaseRole == DatabaseRole.CLIENT ? TestConstants.TEST_CLIENT_NODE_GROUP
                                : TestConstants.TEST_ROOT_NODE_GROUP);
                newProperties.setProperty(ParameterConstants.EXTERNAL_ID,
                        databaseRole == DatabaseRole.ROOT ? TestConstants.TEST_ROOT_EXTERNAL_ID
                                : TestConstants.TEST_CLIENT_EXTERNAL_ID);
                newProperties.setProperty(ParameterConstants.MY_URL, "http://localhost:8888/sync");
                newProperties.setProperty(ParameterConstants.REGISTRATION_URL,
                        databaseRole == DatabaseRole.CLIENT ? "http://localhost:8888/sync" : "");
                newProperties.setProperty(ParameterConstants.ENGINE_NAME, databaseRole.name().toLowerCase());

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
                logger.info("Could not find " + propertiesFile.getAbsolutePath()
                        + ". Using all of the default properties");
            }
            return properties;
        } catch (IOException ex) {
            throw new RuntimeException(ex);
        }
    }

    protected static String[] getRootDbTypes(String testPrefix) {
        Properties properties = getTestProperties(testPrefix);
        return StringUtils.split(properties.getProperty(testPrefix + ".root"), ",");
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
