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

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.SymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.springframework.jdbc.core.JdbcTemplate;

public class AbstractIntegrationTest {

    static final Log logger = LogFactory.getLog(AbstractIntegrationTest.class);

    protected String client;
    protected String root;
    protected JdbcTemplate rootJdbcTemplate;
    protected JdbcTemplate clientJdbcTemplate;
    protected static boolean standalone = false;

    public AbstractIntegrationTest(String client, String root) {
        this.client = client;
        this.root = root;
    }

    public AbstractIntegrationTest() throws Exception {
        if (!standalone) {
            logger.info("Running test in standalone mode");
            standalone = true;
            String[] databases = TestSetupUtil.lookupDatabasePairs(DatabaseTestSuite.DEFAULT_TEST_PREFIX).iterator()
                    .next();
            TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT, databases[0], databases[1]);
        }
    }

    protected SymmetricEngine getRootEngine() {
        return TestSetupUtil.getRootEngine();
    }

    protected SymmetricEngine getClientEngine() {
        return TestSetupUtil.getClientEngine();
    }

    protected IDbDialect getRootDbDialect() {
        return AppUtils.find(Constants.DB_DIALECT, getRootEngine());
    }

    protected IDbDialect getClientDbDialect() {
        return AppUtils.find(Constants.DB_DIALECT, getClientEngine());
    }

    protected String printRootAndClientDatabases() {
        return " The root database is " + root + " and the client database is " + client + ".";
    }
    
    @SuppressWarnings("unchecked")
    protected <T> T findOnClient(String name) {
        return (T) AppUtils.find(name, getClientEngine());
    }
    
    @SuppressWarnings("unchecked")
    protected <T> T findOnRoot(String name) {
        return (T) AppUtils.find(name, getRootEngine());
    }

    @Before
    public void setupTemplates() {
        rootJdbcTemplate = new JdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE, getRootEngine()));
        clientJdbcTemplate = new JdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE, getClientEngine()));
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (standalone) {
            standalone = false;
            logger.info("Cleaning up after test in standalone mode");
            TestSetupUtil.cleanup();
        }
    }

}
