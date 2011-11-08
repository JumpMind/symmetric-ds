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

import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.springframework.jdbc.core.JdbcTemplate;

public class AbstractIntegrationTest extends AbstractTest {

    protected final Log logger = LogFactory.getLog(getClass());

    protected String client;
    protected String root;
    protected JdbcTemplate rootJdbcTemplate;
    protected JdbcTemplate clientJdbcTemplate;
    protected static boolean standalone = true;
    
    protected void printHelpfulDebugInfo(JdbcTemplate jdbc) {        
        int count = 10;
        System.out.println("********** PRINTING DEBUG INFO ");
        
        System.out.println("********** DATA");
        for (Map<String, Object> item : jdbc.queryForList("select * from sym_data order by data_id desc")) {
            System.out.println(item);
            if (count-- == 0) {
                break;
            }
        }
        
        count = 10;
        System.out.println("********** EVENT");
        for (Map<String, Object> item : jdbc.queryForList("select * from sym_data_event order by data_id desc")) {
            System.out.println(item);
            if (count-- == 0) {
                break;
            }
        }
        
        count = 10;
        System.out.println("********** OUTGOING BATCH");
        for (Map<String, Object> item : jdbc.queryForList("select * from sym_outgoing_batch order by batch_id desc")) {
            System.out.println(item);
            if (count-- == 0) {
                break;
            }
        }
        
        count = 10;
        System.out.println("********** GAP");
        for (Map<String, Object> item : jdbc.queryForList("select * from sym_data_gap order by start_id desc")) {
            System.out.println(item);
            if (count-- == 0) {
                break;
            }
        }
        
        System.out.println("********** DONE");
    }

    public void init(String client, String root) {
        this.client = client;
        this.root = root;
    }

    public AbstractIntegrationTest() throws Exception {
        try {            
            if (standalone) {
                String[] databases = TestSetupUtil.lookupDatabasePairs(
                        DatabaseTestSuite.DEFAULT_TEST_PREFIX).iterator().next();            
                init(databases[0], databases[1]);
            }
            
        } catch (Exception e) {
            logger.error(e,e);
            throw e;
        }
    }
    
    @Override
    protected String printDatabases() {
        return printRootAndClientDatabases();
    }

    protected ISymmetricEngine getRootEngine() {
        return TestSetupUtil.getRootEngine();
    }

    protected ISymmetricEngine getClientEngine() {
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
        rootJdbcTemplate = new JdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE,
                getRootEngine()));
        clientJdbcTemplate = new JdbcTemplate((DataSource) AppUtils.find(Constants.DATA_SOURCE,
                getClientEngine()));        
    }
    
    @BeforeClass
    public static void standaloneSetup() throws Exception {
        if (standalone) {
            String[] databases = TestSetupUtil.lookupDatabasePairs(
                    DatabaseTestSuite.DEFAULT_TEST_PREFIX).iterator().next();            
            TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX,
                    TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT, databases[0], databases[1]);
        }
    }
    
    protected boolean clientPush() {
        int tries = 0;
        boolean pushed = false;
        while (!pushed && tries < 10) {
            pushed = getClientEngine().push().wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pushed;
    }
    
    protected boolean clientPull() {
        int tries = 0;
        boolean pulled = false;
        while (!pulled && tries < 10) {
            pulled = getClientEngine().pull().wasDataProcessed();
            AppUtils.sleep(100);
            tries++;
        }
        return pulled;
    }

    @AfterClass
    public static void cleanup() throws Exception {
        if (standalone) {
            LogFactory.getLog(AbstractDatabaseTest.class).info(
                    "Cleaning up after test in standalone mode");
            TestSetupUtil.cleanup();
        }
    }

}