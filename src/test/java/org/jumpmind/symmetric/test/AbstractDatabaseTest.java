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
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.springframework.jdbc.core.JdbcTemplate;

public class AbstractDatabaseTest {

    static final Log logger = LogFactory.getLog(AbstractDatabaseTest.class);

    protected String database;
    static boolean standalone = false;

    public AbstractDatabaseTest(String dbName) {
        this.database = dbName;
    }

    public AbstractDatabaseTest() throws Exception {
        if (!standalone) {
            logger.info("Running test in standalone mode");
            standalone = true;
            TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT,
                    null, TestSetupUtil.getRootDbTypes(DatabaseTestSuite.DEFAULT_TEST_PREFIX)[0]);
        }
    }

    protected SymmetricEngine getSymmetricEngine() {
        return TestSetupUtil.getRootEngine();
    }

    protected IParameterService getParameterService() {
        return AppUtils.find(Constants.PARAMETER_SERVICE, getSymmetricEngine());
    }

    protected IDbDialect getDbDialect() {
        return AppUtils.find(Constants.DB_DIALECT, getSymmetricEngine());
    }

    protected DataSource getDataSource() {
        return AppUtils.find(Constants.DATA_SOURCE, getSymmetricEngine());
    }

    protected JdbcTemplate getJdbcTemplate() {
        return new JdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE, getSymmetricEngine()));
    }

    @SuppressWarnings("unchecked")
    protected <T> T find(String name) {
        return (T) AppUtils.find(name, getSymmetricEngine());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (standalone) {
            logger.info("Cleaning up after test in standalone mode");
            TestSetupUtil.cleanup();
            standalone = false;
        }
    }

    protected void cleanSlate(final String... tableName) {
        for (String table : tableName) {
            getJdbcTemplate().update("delete from " + table);
        }
    }

    protected void assertTrue(boolean condition, String message) {
        Assert.assertTrue(message, condition);
    }

    protected void assertFalse(boolean condition, String message) {
        Assert.assertFalse(message, condition);
    }

    protected void assertNotNull(Object condition, String message) {
        Assert.assertNotNull(message, condition);
    }
    
    protected void assertNull(Object condition) {
        Assert.assertNull(condition);
    }
    
    protected void assertNotNull(Object condition) {
        Assert.assertNotNull(condition);
    }

    protected void assertNull(Object condition, String message) {
        Assert.assertNull(message, condition);
    }

    protected void assertFalse(boolean condition) {
        Assert.assertFalse(condition);
    }

    protected void assertTrue(boolean condition) {
        Assert.assertTrue(condition);
    }
    
    protected void assertEquals(Object actual, Object expected) {
        Assert.assertEquals(expected, actual);
    }

    protected void assertEquals(Object actual, Object expected, String message) {
        Assert.assertEquals(message, expected, actual);
    }
    
    protected void assertNotSame(Object actual, Object expected, String message) {
        Assert.assertNotSame(message, expected, actual);
    }        
    
}
