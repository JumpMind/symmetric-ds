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

import java.util.Collection;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized.Parameters;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(ParameterizedSuite.class)
@SuiteClasses( { SimpleIntegrationTest.class, LoadFromClientIntegrationTest.class, CleanupTest.class })
/**
 * 
 */
public class IntegrationTestSuite {

    static final String TEST_PREFIX = "test";

    @Parameters
    public static Collection<String[]> lookupClientServerDatabases() {
        return TestSetupUtil.lookupDatabasePairs(TEST_PREFIX);
    }

    String root;
    String client;
    
    public void init(String client, String root) {
        this.client = client;
        this.root = root;
    }

    @Test
    public void setup() throws Exception {
        AbstractIntegrationTest.standalone = false;
        TestSetupUtil.setup(TEST_PREFIX, TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT, client, root);
    }

}