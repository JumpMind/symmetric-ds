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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Assert;
import org.junit.Test;

/**
 * 
 */
public class LoadFromClientIntegrationTest extends AbstractIntegrationTest {

    static final Log logger = LogFactory.getLog(LoadFromClientIntegrationTest.class);

    public LoadFromClientIntegrationTest() throws Exception {
    }

    @Test (timeout = 30000)
    public void registerClientWithRoot() {
        getRootEngine().openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        getClientEngine().start();
        while (!getClientEngine().isRegistered()) {
            getClientEngine().pull();
        }
        String result = getClientEngine().reloadNode("00000");
        Assert.assertTrue(result, result.startsWith("Successfully opened initial load for node"));
        
        getClientEngine().route();
        while (getClientEngine().push().wasDataProcessed()) {
            AppUtils.sleep(5);
        }
        Assert.assertEquals(0, getInitialLoadEnabled());
    }
    
    protected int getInitialLoadEnabled() {
        return clientJdbcTemplate.queryForInt("select initial_load_enabled from sym_node_security where node_id='00000'");
    }

}