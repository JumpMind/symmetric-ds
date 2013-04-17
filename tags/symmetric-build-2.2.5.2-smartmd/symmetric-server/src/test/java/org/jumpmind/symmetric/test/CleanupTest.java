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
import org.junit.Test;

/**
 * 
 */
public class CleanupTest {
    static final Log logger = LogFactory.getLog(SimpleIntegrationTest.class);

    String client;
    String root;

    public CleanupTest() throws Exception {
    }

    public void init(String client, String root) {
        this.client = client;
        this.root = root;
    }

    public void init(String database) {
        this.root = database;
    }
    
    @Test
    public void cleanup() throws Exception {
        TestSetupUtil.cleanup();
    }
}