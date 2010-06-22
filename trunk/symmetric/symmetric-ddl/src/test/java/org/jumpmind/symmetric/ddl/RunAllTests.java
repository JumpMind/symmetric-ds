package org.jumpmind.symmetric.ddl;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import org.jumpmind.symmetric.ddl.alteration.TestAlterationAlgorithm;
import org.jumpmind.symmetric.ddl.alteration.TestModelComparator;
import org.jumpmind.symmetric.ddl.dynabean.TestDynaSqlQueries;
import org.jumpmind.symmetric.ddl.io.TestAlteration;
import org.jumpmind.symmetric.ddl.io.TestConstraints;
import org.jumpmind.symmetric.ddl.io.TestDataReaderAndWriter;
import org.jumpmind.symmetric.ddl.io.TestDatabaseIO;
import org.jumpmind.symmetric.ddl.io.TestDatatypes;
import org.jumpmind.symmetric.ddl.io.TestMisc;
import org.jumpmind.symmetric.ddl.io.converters.TestDateConverter;
import org.jumpmind.symmetric.ddl.io.converters.TestTimeConverter;
import org.jumpmind.symmetric.ddl.model.TestArrayAccessAtTable;
import org.jumpmind.symmetric.ddl.platform.TestAxionPlatform;
import org.jumpmind.symmetric.ddl.platform.TestCloudscapePlatform;
import org.jumpmind.symmetric.ddl.platform.TestDB2Platform;
import org.jumpmind.symmetric.ddl.platform.TestDerbyPlatform;
import org.jumpmind.symmetric.ddl.platform.TestFirebirdPlatform;
import org.jumpmind.symmetric.ddl.platform.TestHsqlDbPlatform;
import org.jumpmind.symmetric.ddl.platform.TestInterbasePlatform;
import org.jumpmind.symmetric.ddl.platform.TestMSSqlPlatform;
import org.jumpmind.symmetric.ddl.platform.TestMaxDbPlatform;
import org.jumpmind.symmetric.ddl.platform.TestMcKoiPlatform;
import org.jumpmind.symmetric.ddl.platform.TestMySql50Platform;
import org.jumpmind.symmetric.ddl.platform.TestMySqlPlatform;
import org.jumpmind.symmetric.ddl.platform.TestOracle8Platform;
import org.jumpmind.symmetric.ddl.platform.TestOracle9Platform;
import org.jumpmind.symmetric.ddl.platform.TestPlatformUtils;
import org.jumpmind.symmetric.ddl.platform.TestPostgresqlPlatform;
import org.jumpmind.symmetric.ddl.platform.TestSapDbPlatform;
import org.jumpmind.symmetric.ddl.platform.TestSqlBuilder;
import org.jumpmind.symmetric.ddl.platform.TestSybasePlatform;

import junit.framework.Test;
import junit.framework.TestCase;
import junit.framework.TestSuite;

/**
 * Helper class to run all DdlUtils tests.
 * 
 * @version $Revision: 289996 $
 */
public class RunAllTests extends TestCase
{
    /**
     * Creates a new instance.
     * 
     * @param name The name of the test case
     */
    public RunAllTests(String name)
    {
        super(name);
    }

    /**
     * Runs the test cases on the commandline using the text ui.
     * 
     * @param args The invocation arguments
     */
    public static void main(String[] args)
    {
        junit.textui.TestRunner.run(suite());
    }

    /**
     * Returns a test suite containing all test cases.
     * 
     * @return The test suite
     */
    public static Test suite()
    {
        TestSuite suite = new TestSuite("DdlUtils tests");

        // tests that don't need a live database
        suite.addTestSuite(TestArrayAccessAtTable.class);
        suite.addTestSuite(TestSqlBuilder.class);
        suite.addTestSuite(TestPlatformUtils.class);
        suite.addTestSuite(TestDatabaseIO.class);
        suite.addTestSuite(TestDataReaderAndWriter.class);
        suite.addTestSuite(TestDateConverter.class);
        suite.addTestSuite(TestTimeConverter.class);
        suite.addTestSuite(TestAxionPlatform.class);
        suite.addTestSuite(TestCloudscapePlatform.class);
        suite.addTestSuite(TestDB2Platform.class);
        suite.addTestSuite(TestDerbyPlatform.class);
        suite.addTestSuite(TestFirebirdPlatform.class);
        suite.addTestSuite(TestHsqlDbPlatform.class);
        suite.addTestSuite(TestInterbasePlatform.class);
        suite.addTestSuite(TestMaxDbPlatform.class);
        suite.addTestSuite(TestMcKoiPlatform.class);
        suite.addTestSuite(TestMSSqlPlatform.class);
        suite.addTestSuite(TestMySqlPlatform.class);
        suite.addTestSuite(TestMySql50Platform.class);
        suite.addTestSuite(TestOracle8Platform.class);
        suite.addTestSuite(TestOracle9Platform.class);
        suite.addTestSuite(TestPostgresqlPlatform.class);
        suite.addTestSuite(TestSapDbPlatform.class);
        suite.addTestSuite(TestSybasePlatform.class);
        suite.addTestSuite(TestModelComparator.class);
        suite.addTestSuite(TestAlterationAlgorithm.class);

        // tests that need a live database
        if (System.getProperty(TestDatabaseWriterBase.JDBC_PROPERTIES_PROPERTY) != null)
        {
            suite.addTestSuite(TestDynaSqlQueries.class);
            suite.addTestSuite(TestDatatypes.class);
            suite.addTestSuite(TestConstraints.class);
            suite.addTestSuite(TestAlteration.class);
            suite.addTestSuite(TestMisc.class);
        }

        return suite;
    }
}
