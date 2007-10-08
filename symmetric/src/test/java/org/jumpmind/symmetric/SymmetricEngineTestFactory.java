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
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import javax.sql.DataSource;

import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.TestConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.testng.Assert;

public class SymmetricEngineTestFactory {
    
    private static SymmetricEngine mySqlEngine1;

    private static SymmetricEngine mySqlEngine2;

    private static SymmetricEngine oracleEngine1;

    static final String ENGINE_1_PROPERTIES = "symmetric-mysql-engine-1.properties";
    
    static final String ENGINE_1_CONTINUOUS_PROPERTIES = "symmetric-mysql-engine-1-continuous.properties";

    static final String ENGINE_2_PROPERTIES = "symmetric-mysql-engine-2.properties";

    static final String ORACLE_ENGINE_1_PROPERTIES = "symmetric-oracle-engine-1.properties";

    public static SymmetricEngine getMySqlTestEngine1(String engineScript) {

        if (mySqlEngine1 == null) {
            File file = assertThatFileExists(new File(System
                    .getProperty("user.home")
                    + "/" + ENGINE_1_PROPERTIES));
            mySqlEngine1 = new SymmetricEngine("classpath:/"
                    + ENGINE_1_PROPERTIES, "file:" + file.getAbsolutePath());
            resetSchemaForEngine(mySqlEngine1, engineScript);
            mySqlEngine1.start();
        }
        return mySqlEngine1;
    }
    
    public static SymmetricEngine getContinuousTestEngine() {
        if (mySqlEngine1 == null) {
            File file = assertThatFileExists(new File(System
                    .getProperty("user.home")
                    + "/" + ENGINE_1_PROPERTIES));
            mySqlEngine1 = new SymmetricEngine("classpath:/"
                    + ENGINE_1_CONTINUOUS_PROPERTIES, "file:" + file.getAbsolutePath());
            resetSchemaForEngine(mySqlEngine1, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT);
            mySqlEngine1.start();
        }
        return mySqlEngine1;
    }    

    public static SymmetricEngine getMySqlTestEngine2(String engineScript) {

        if (mySqlEngine2 == null) {
            File file = assertThatFileExists(new File(System
                    .getProperty("user.home")
                    + "/" + ENGINE_2_PROPERTIES));
            mySqlEngine2 = new SymmetricEngine("classpath:/"
                    + ENGINE_2_PROPERTIES, "file:" + file.getAbsolutePath());
            resetSchemaForEngine(mySqlEngine2, engineScript);
            mySqlEngine2.start();
        }
        return mySqlEngine2;
    }

    public static SymmetricEngine getOracleTestEngine1(String engineScript) {

        if (oracleEngine1 == null) {
            File file = assertThatFileExists(new File(System
                    .getProperty("user.home")
                    + "/" + ORACLE_ENGINE_1_PROPERTIES));
            oracleEngine1 = new SymmetricEngine("classpath:/"
                    + ORACLE_ENGINE_1_PROPERTIES, "file:/"
                    + file.getAbsolutePath());
            // dropOracleSequences(oracleEngine1);
            resetSchemaForEngine(oracleEngine1, engineScript);
            oracleEngine1.start();

        }
        return oracleEngine1;
    }

    public static SymmetricEngine[] getUnitTestableEngines() {
        List<SymmetricEngine> engines = new ArrayList<SymmetricEngine>();
        if (Boolean.TRUE.toString().equalsIgnoreCase(
                System.getProperty("symmetric.include.oracle.tests"))) {
            engines
                    .add(getOracleTestEngine1(TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT));
        }

        engines
                .add(getMySqlTestEngine1(TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT));
        return engines.toArray(new SymmetricEngine[engines.size()]);
    }

    public static void resetSchemasAndEngines() {
        mySqlEngine1 = null;
        mySqlEngine2 = null;
        oracleEngine1 = null;
    }

    private static void resetSchemaForEngine(SymmetricEngine engine,
            String setupScript) {
        DataSource ds = (DataSource) engine.getApplicationContext().getBean(
                Constants.DATA_SOURCE);
        try {
            IDbDialect dialect = (IDbDialect) engine.getApplicationContext()
                    .getBean(Constants.DB_DIALECT);
            Platform platform = dialect.getPlatform();
            Database testDb = getTestDatabase();
            platform.dropTables(testDb, true);
            dialect.purge();
            platform.createTables(testDb, false, true);
            new SqlScript(getResource(TestConstants.TEST_DROP_ALL_SCRIPT), ds,
                    false).execute();
            // Need to init the table before running insert statements
            ((IBootstrapService) engine.getApplicationContext().getBean(
                    Constants.BOOTSTRAP_SERVICE)).init();
            if (setupScript != null) {
                new SqlScript(getResource(setupScript), ds, true).execute();
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    private static void dropOracleSequences(SymmetricEngine engine) {
        DataSource ds = (DataSource) engine.getApplicationContext().getBean(
                Constants.DATA_SOURCE);
        try {
            new SqlScript(getResource(TestConstants.TEST_DROP_SEQ_SCRIPT), ds,
                    false).execute();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

    }

    private static URL getResource(String resource) {
        return SymmetricEngineTestFactory.class.getResource(resource);
    }

    protected static Database getTestDatabase() throws IOException {
        return new DatabaseIO().read(new InputStreamReader(getResource(
                "/test-tables-ddl.xml").openStream()));
    }

    private static File assertThatFileExists(File file) {
        Assert
                .assertTrue(
                        file.exists() && file.isFile(),
                        file.getAbsolutePath()
                                + " does not exist. Please create and populate it with database connection properties for unit testing.");
        return file;
    }
}
