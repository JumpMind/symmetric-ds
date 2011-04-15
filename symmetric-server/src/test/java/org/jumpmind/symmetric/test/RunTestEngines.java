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

import org.jumpmind.symmetric.ISymmetricEngine;

/**
 * Simple test utility class to help with stand-alone testing. Run this class
 * from your development environment to get a SymmetricDS client and server you
 * can play with.
 *
 * 
 */
public class RunTestEngines {

    public static void main(String[] args) throws Exception {
        String[] databases = TestSetupUtil.lookupDatabasePairs(DatabaseTestSuite.DEFAULT_TEST_PREFIX).iterator().next();
        TestSetupUtil.setup(DatabaseTestSuite.DEFAULT_TEST_PREFIX, TestConstants.TEST_ROOT_DOMAIN_SETUP_SCRIPT,
                databases[0], databases[1]);
        ISymmetricEngine root = TestSetupUtil.getRootEngine();
        ISymmetricEngine client = TestSetupUtil.getClientEngine();
        root.openRegistration(TestConstants.TEST_CLIENT_NODE_GROUP, TestConstants.TEST_CLIENT_EXTERNAL_ID);
        client.start();
        while (true) {
            client.pull();
            client.push();
            client.heartbeat(false);
            root.heartbeat(false);
            Thread.sleep(5000);
        }
    }

}