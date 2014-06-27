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

import java.util.Date;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.simple.SimpleJdbcTemplate;
import org.springframework.transaction.support.TransactionTemplate;

/**
 * 
 */
public class AbstractDatabaseTest extends AbstractTest {

    protected Log logger = LogFactory.getLog(getClass());

    private String database = TestSetupUtil.getRootDbTypes(DatabaseTestSuite.DEFAULT_TEST_PREFIX)[0];
    
    static boolean standalone = true;
   
    public void init(String database) {
        this.database = database;
    }
    
    @Override
    protected String printDatabases() {
        return getDatabase();
    }

    public AbstractDatabaseTest() throws Exception {
        if (standalone) {
            logger.info("Running test in standalone mode against " + database);
            standalone = false;
            TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_CONTINUOUS_SETUP_SCRIPT,
                    null, database);
        }
    }
    
    public String getDatabase() {
        return database;
    }

    protected String printDatabase() {
        return " The database we are testing against is " + database + ".";
    }

    protected ISymmetricEngine getSymmetricEngine() {
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

    protected IRegistrationService getRegistrationService() {
        return AppUtils.find(Constants.REGISTRATION_SERVICE, getSymmetricEngine());
    }
    
    protected IDataService getDataService() {
        return AppUtils.find(Constants.DATA_SERVICE, getSymmetricEngine());
    }


    protected INodeService getNodeService() {
        return AppUtils.find(Constants.NODE_SERVICE, getSymmetricEngine());
    }

    protected IRouterService getRouterService() {
        return AppUtils.find(Constants.ROUTER_SERVICE, getSymmetricEngine());
    }

    protected ITriggerRouterService getTriggerRouterService() {
        return AppUtils.find(Constants.TRIGGER_ROUTER_SERVICE, getSymmetricEngine());
    }

    protected IOutgoingBatchService getOutgoingBatchService() {
        return AppUtils.find(Constants.OUTGOING_BATCH_SERVICE, getSymmetricEngine());
    }
    
    protected IIncomingBatchService getIncomingBatchService() {
        return AppUtils.find(Constants.INCOMING_BATCH_SERVICE, getSymmetricEngine());
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

    protected void cleanSlate(final String... tableNames) {
        if (tableNames != null) {
            for (String tableName : tableNames) {
                getDbDialect().truncateTable(tableName);
            }
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
    
    protected void assertNumberOfRows(int rows, String tableName) {
        Assert.assertEquals(tableName + " had an unexpected number of rows", rows, getJdbcTemplate().queryForInt("select count(*) from " + tableName));
    }
    
    protected void forceRebuildOfTrigers() {
        getJdbcTemplate().update("update sym_trigger set last_update_time=?", new Date());
        getTriggerRouterService().syncTriggers();
    }
    
    protected int countData() {
        return getDataService().countDataInRange(-1, Integer.MAX_VALUE);
    }

}