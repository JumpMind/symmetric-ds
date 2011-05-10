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

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Test;

/**
 * 
 */
public class ClusterServiceTest extends AbstractDatabaseTest {

    public ClusterServiceTest() throws Exception {
        super();
    }

    @Test
    public void testLock() throws Exception {
        final IClusterService service = (IClusterService) find(Constants.CLUSTER_SERVICE);
        final String serverId = service.getServerId();
        assertTrue(service.lock(ClusterConstants.PURGE_INCOMING), "Could not lock for PURGE");
        assertEquals(countActivePurgeLocks(), 1, "Could not find the lock in the database.");
        service.setServerId("anotherServer");
        assertFalse(service.lock(ClusterConstants.PURGE_INCOMING), "Should not have been able to lock for PURGE");
        service.setServerId(serverId);
        assertTrue(service.lock(ClusterConstants.PURGE_INCOMING), "Could not lock for PURGE.  Should have been able to break the lock because the server id is the same.");
        service.unlock(ClusterConstants.PURGE_INCOMING);
        assertEquals(countActivePurgeLocks(), 0, "Could not find the lock in the database.");
    }

    private int countActivePurgeLocks() {
        return getJdbcTemplate().queryForInt(
                "select count(*) from sym_lock where lock_action=? and lock_time is not null",
                new Object[] { ClusterConstants.PURGE_INCOMING });
    }
}