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
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRoutingService;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

public class AbstractDatabaseTest {

    protected Log logger = LogFactory.getLog(getClass());

    protected String database;
    static boolean standalone = false;

    public AbstractDatabaseTest(String dbName) {
        this.database = dbName;
    }

    public AbstractDatabaseTest() throws Exception {
        if (!standalone) {
            database = TestSetupUtil.getRootDbTypes(DatabaseTestSuite.DEFAULT_TEST_PREFIX)[0];
            logger.info("Running test in standalone mode against " + database);
            standalone = true;
            TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT,
                    null, database);
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

    protected IConfigurationService getConfigurationService() {
        return AppUtils.find(Constants.CONFIG_SERVICE, getSymmetricEngine());
    }

    protected IBootstrapService getBootstrapService() {
        return AppUtils.find(Constants.BOOTSTRAP_SERVICE, getSymmetricEngine());
    }
    
    protected IRegistrationService getRegistrationService() {
        return AppUtils.find(Constants.REGISTRATION_SERVICE, getSymmetricEngine());
    }
    
    protected INodeService getNodeService() {
        return AppUtils.find(Constants.NODE_SERVICE, getSymmetricEngine());
    }

    protected IRoutingService getRoutingService() {
        return AppUtils.find(Constants.ROUTING_SERVICE, getSymmetricEngine());
    }

    protected IOutgoingBatchService getOutgoingBatchService() {
        return AppUtils.find(Constants.OUTGOING_BATCH_SERVICE, getSymmetricEngine());
    }

    protected DataSource getDataSource() {
        return AppUtils.find(Constants.DATA_SOURCE, getSymmetricEngine());
    }

    protected TransactionTemplate getTransactionTemplate() {
        return AppUtils.find(Constants.TRANSACTION_TEMPLATE, getSymmetricEngine());
    }

    protected JdbcTemplate getJdbcTemplate() {
        return new JdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE, getSymmetricEngine()));
    }

    protected SimpleJdbcTemplate getSimpleJdbcTemplate() {
        return new SimpleJdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE, getSymmetricEngine()));
    }

    @SuppressWarnings("unchecked")
    protected <T> T find(String name) {
        return (T) AppUtils.find(name, getSymmetricEngine());
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (standalone) {
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
