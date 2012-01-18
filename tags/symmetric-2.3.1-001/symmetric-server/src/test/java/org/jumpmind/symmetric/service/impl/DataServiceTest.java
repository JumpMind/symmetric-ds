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
package org.jumpmind.symmetric.service.impl;

import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Test;

public class DataServiceTest extends AbstractDatabaseTest {

    public DataServiceTest() throws Exception {
        super();
    }

    @Test
    public void verifyProxyInterface() {
        IDataService service = getSymmetricEngine().getDataService();
        Assert.assertNotSame(DataLoaderService.class, service.getClass());
    }
    
    @Test
    public void testGetHeartbeatListeners() throws Exception {
        DataService ds = new DataService();
        MockHeartbeatListener one = new MockHeartbeatListener();
        MockHeartbeatListener two = new MockHeartbeatListener();
        one.timeBetweenHeartbeats = 10;
        two.timeBetweenHeartbeats = 0;
        ds.addHeartbeatListener(one);
        ds.addHeartbeatListener(two);
        List<IHeartbeatListener> listeners = ds.getHeartbeatListeners(false);
        Assert.assertEquals(2, listeners.size());
        listeners = ds.getHeartbeatListeners(false);
        Assert.assertEquals(2, listeners.size());
        ds.updateLastHeartbeatTime(listeners);
        listeners = ds.getHeartbeatListeners(false);
        Assert.assertEquals(1, listeners.size());
        Assert.assertEquals(two, listeners.get(0));
        one.timeBetweenHeartbeats = 1;
        AppUtils.sleep(1000);
        listeners = ds.getHeartbeatListeners(false);
        Assert.assertEquals(2, listeners.size());
    }

    class MockHeartbeatListener implements IHeartbeatListener {
        protected boolean heartbeated = false;
        protected long timeBetweenHeartbeats;

        public long getTimeBetweenHeartbeatsInSeconds() {
            return timeBetweenHeartbeats;
        }

        public void heartbeat(Node me, Set<Node> children) {
            heartbeated = true;
        }

        public boolean isAutoRegister() {
            return false;
        }

    }

}