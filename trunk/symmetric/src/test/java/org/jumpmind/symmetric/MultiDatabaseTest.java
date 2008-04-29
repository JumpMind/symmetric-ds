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
package org.jumpmind.symmetric;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.load.DataLoaderTest;
import org.jumpmind.symmetric.service.impl.DataLoaderServiceTest;
import org.testng.annotations.Factory;

/**
 * Run this test to run all the tests against all the configured databases.
 */
public class MultiDatabaseTest {

    private static final Log logger = LogFactory.getLog(MultiDatabaseTest.class);

    enum DatabaseRole {
        CLIENT, ROOT
    };

    public Object[][] getClientAndRootCombos() {

        Properties properties = getTestProperties();
        String[] clientDatabaseTypes = StringUtils.split(properties.getProperty("test.client"), ",");
        String[] rootDatabaseTypes = StringUtils.split(properties.getProperty("test.root"), ",");

        Object[][] clientAndRootCombos = new Object[rootDatabaseTypes.length * clientDatabaseTypes.length][2];

        int index = 0;
        for (String rootDatabaseType : rootDatabaseTypes) {
            for (String clientDatabaseType : clientDatabaseTypes) {
                clientAndRootCombos[index][0] = clientDatabaseType;
                clientAndRootCombos[index++][1] = rootDatabaseType;
            }
        }
        return clientAndRootCombos;

    }

    @Factory
    public Object[] createTests() throws Exception {
        List<Object> tests2Run = new ArrayList<Object>();
        Object[][] clientAndRootCombos = getClientAndRootCombos();
        for (Object[] objects : clientAndRootCombos) {
            // TODO temporarily disable to see if the mismash of test methods causes tests to fail.
            tests2Run.addAll(createDatabaseTests(objects[1].toString()));
            //tests2Run.addAll(createIntegrationTests(objects[0].toString(), objects[1].toString()));
        }

        return tests2Run.toArray(new Object[tests2Run.size()]);
    }

    public List<? extends AbstractTest> createIntegrationTests(String clientDatabaseType,
            String rootDatabaseType) throws Exception {
        List<AbstractIntegrationTest> tests2Run = new ArrayList<AbstractIntegrationTest>();
        File clientFile = writeTempPropertiesFileFor(clientDatabaseType, DatabaseRole.CLIENT);
        File rootFile = writeTempPropertiesFileFor(rootDatabaseType, DatabaseRole.ROOT);
        if (clientFile != null && rootFile != null) {
            addAbstractIntegrationTests(clientFile, rootFile, tests2Run);
        }
        return tests2Run;
    }

    /**
     * If you want to add any additional node 2 node tests, subclass from AbstractIntegrationTest and add the test here.  The reason
     * for the anonymous inner class trick is so TestNG actually runs tests sequentially (I found that if the same test is added
     * more than once each of the same test methods are run once for each instance of the class instead of running once test to completion, then
     * another.) 
     */
    protected void addAbstractIntegrationTests(final File clientFile, final File rootFile,
            List<AbstractIntegrationTest> tests2Run) {
        tests2Run.add(new IntegrationTest() {
            @Override
            File getClientFile() {
                return clientFile;
            }

            @Override
            File getRootFile() {
                return rootFile;
            }

        });
    }

    public List<? extends AbstractTest> createDatabaseTests(String rootDatabaseType) throws Exception {
        List<AbstractDatabaseTest> tests2Run = new ArrayList<AbstractDatabaseTest>();
        final File rootFile = writeTempPropertiesFileFor(rootDatabaseType, DatabaseRole.ROOT);
        if (rootFile != null) {
            addAbstractDatabaseTests(rootFile, tests2Run);
        }
        return tests2Run;
    }

    protected void addAbstractDatabaseTests(final File rootFile, List<AbstractDatabaseTest> tests2Run) {
        tests2Run.add(new DataLoaderTest() {
            @Override
            File getSymmetricFile() {
                return rootFile;
            }
        });

        tests2Run.add(new DataLoaderServiceTest() {
            @Override
            File getSymmetricFile() {
                return rootFile;
            }
        });

        /* Cannot add tests that have dependent methods because they are not 
         * ordered correctly.
        tests2Run.add(new DbTriggerTest() {
            @Override
            File getSymmetricFile() {
                return rootFile;
            }
        });
        */
    }

    protected static File writeTempPropertiesFileFor(String databaseType, DatabaseRole databaseRole) {
        try {
            Properties properties = getTestProperties();
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
                newProperties.setProperty("symmetric.runtime.group.id",
                        databaseRole == DatabaseRole.CLIENT ? TestConstants.TEST_CLIENT_NODE_GROUP
                                : TestConstants.TEST_ROOT_NODE_GROUP);
                newProperties.setProperty("symmetric.runtime.external.id",
                        databaseRole == DatabaseRole.ROOT ? TestConstants.TEST_ROOT_EXTERNAL_ID
                                : TestConstants.TEST_CLIENT_EXTERNAL_ID);
                newProperties.setProperty("symmetric.runtime.my.url", "internal://"
                        + databaseRole.name().toLowerCase());
                newProperties.setProperty("symmetric.runtime.registration.url",
                        databaseRole == DatabaseRole.CLIENT ? ("internal://" + DatabaseRole.ROOT.name()
                                .toLowerCase()) : "");
                newProperties.setProperty("symmetric.runtime.engine.name", databaseRole.name().toLowerCase());

                File propertiesFile = File.createTempFile("symmetric-test.", ".properties");
                FileOutputStream os = new FileOutputStream(propertiesFile);
                newProperties.store(os, "generated by the symmetricds unit tests");
                os.close();
                propertiesFile.deleteOnExit();
                return propertiesFile;

            } else {
                logger.error("Could not find a valid connection for " + databaseType);
                return null;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

    }

    private static boolean isConnectionValid(Properties properties) throws Exception {
        try {
            Class.forName(properties.getProperty("db.driver"));
            Connection c = DriverManager.getConnection(properties.getProperty("db.url"), properties
                    .getProperty("db.user"), properties.getProperty("db.password"));
            c.close();
            return true;
        } catch (Exception ex) {
            logger.error("Could not connect to the test database using the url: "
                    + properties.getProperty("db.url") + " and classpath: "
                    + System.getProperty("java.class.path"), ex);
            return false;
        }
    }

    protected static Properties getTestProperties() {
        try {
            final String TEST_PROPERTIES_FILE = "/symmetric-test.properties";
            Properties properties = new Properties();

            properties.load(MultiDatabaseTest.class.getResourceAsStream(TEST_PROPERTIES_FILE));
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

}
