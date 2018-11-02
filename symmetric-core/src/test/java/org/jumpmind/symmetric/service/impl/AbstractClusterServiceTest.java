/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.model.Lock;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class AbstractClusterServiceTest extends AbstractServiceTest {

    @Before
    public void setupForTest() {
        getClusterService().init();
        getParameterService().saveParameter(ParameterConstants.LOCK_WAIT_RETRY_MILLIS, "1", "test");
    }

    @Test
    public void testLockCluster() {
        lock(ClusterConstants.PULL, ClusterConstants.TYPE_CLUSTER, 0);
        // Should allow multiple cluster locks when on same server
        lock(ClusterConstants.PULL, ClusterConstants.TYPE_CLUSTER, 0);
        unlock(ClusterConstants.PULL, ClusterConstants.TYPE_CLUSTER, 0);
    }

    @Test
    public void testLockShare() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);

        // Should allow multiple shared locks and increase shared count
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 2);

        // Should prevent an exclusive lock
        Assert.assertFalse(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE));

        // Releasing shared lock should decrease shared count
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);

        // Releasing final shared lock
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 0);
    }

    @Test
    public void testLockShareAfterExclusive() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 0);
    }

    @Test
    public void testLockShareAbandoned() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);
        getClusterService().init();
        checkUnlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 0, false);
    }

    @Test
    public void testLockExclusive() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);

        // Should prevent a second exclusive lock
        Assert.assertFalse(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE));

        // Should prevent a shared lock
        Assert.assertFalse(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED));

        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        getClusterService().unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE);
    }

    @Test
    public void testLockExclusiveAfterShare() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 0);
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
    }

    @Test
    public void testLockExclusiveAbandoned() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        getClusterService().init();
        checkUnlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0, false);
    }

    @Test
    public void testLockExclusiveWait() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);
        Assert.assertFalse(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 1));
        checkLock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1, false);
        Assert.assertFalse(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED));
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 0);
        Assert.assertTrue(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 1));
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
    }

    @Test
    public void testLockSharedWait() {
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        Assert.assertFalse(getClusterService().lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1));
        checkLock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0, false);
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_EXCLUSIVE, 0);
        lock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 1);
        unlock(ClusterConstants.FILE_SYNC_SHARED, ClusterConstants.TYPE_SHARED, 0);
    }
    
    @Test
    public void testGenerateInstanceId() {
        ClusterService clusterService = (ClusterService) getClusterService();
        {            
            String instanceId = clusterService.generateInstanceId("looooooooooooooooooooooooooooooooooooooooooooong hostname.");
            assertTrue((instanceId.length() <= 60), "");
        }
        {            
            String instanceId = clusterService.generateInstanceId("short hostname");
            assertTrue((instanceId.length() <= 60), "");
        }
        {            
            String instanceId = clusterService.generateInstanceId(null);
            assertTrue((instanceId.length() <= 60), "");
        }
    }

    private void lock(String action, String lockType, int expectedSharedCount) {
        Assert.assertTrue("Expected to obtain lock", getClusterService().lock(action, lockType));
        checkLock(action, lockType, expectedSharedCount, expectedSharedCount > 0);
    }

    private void unlock(String action, String lockType, int expectedSharedCount) {
        getClusterService().unlock(action, lockType);
        checkUnlock(action, lockType, expectedSharedCount, expectedSharedCount > 0);
    }
    
    private Lock checkLock(String action, String lockType, int expectedSharedCount, boolean expectedSharedEnable) {
        Lock lock = getClusterService().findLocks().get(action);
        Assert.assertEquals(lockType, lock.getLockType());
        Assert.assertNotNull(lock.getLockingServerId());
        Assert.assertNotNull(lock.getLockTime());
        Assert.assertEquals(expectedSharedCount, lock.getSharedCount());
        if (expectedSharedCount > 0) {
            Assert.assertEquals(expectedSharedEnable, lock.isSharedEnable());    
        }
        return lock;
    }

    private void checkUnlock(String action, String lockType, int expectedSharedCount, boolean expectedSharedEnable) {
        Lock lock = getClusterService().findLocks().get(action);
        Assert.assertEquals(lockType, lock.getLockType());
        Assert.assertNotNull(lock.getLastLockingServerId());
        Assert.assertNotNull(lock.getLastLockTime());
        if (lockType != ClusterConstants.TYPE_SHARED || lock.getSharedCount() == 0) {
            Assert.assertNull(lock.getLockingServerId());
            Assert.assertNull(lock.getLockTime());
            Assert.assertFalse(lock.isSharedEnable());
        }
        Assert.assertEquals(expectedSharedCount, lock.getSharedCount());
    }

}
