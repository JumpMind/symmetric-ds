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
package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.junit.Test;

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

    @Test
    public void testOtherNodeLock() throws Exception {
        final String ID_ONE = "00020";
        final String ID_TWO = "00010";


        final IClusterService service = (IClusterService) find(Constants.CLUSTER_SERVICE);
        final String serverId = service.getServerId();
        service.initLockTable("OTHER", ID_ONE);
        service.initLockTable("OTHER", ID_TWO);
        assertTrue(service.lock("OTHER", ID_ONE), "Could not lock for OTHER " + ID_ONE);
        service.setServerId("anotherServer");
        assertFalse(service.lock("OTHER", ID_ONE), "Should not have been able to lock for OTHER "
                + ID_ONE);
        assertTrue(service.lock("OTHER", ID_TWO), "Could not lock for OTHER " + ID_TWO);
        service.setServerId(serverId);
        service.unlock("OTHER", ID_ONE);
        assertTrue(service.lock("OTHER", ID_ONE), "Could not lock for OTHER " + ID_ONE);

    }

    private int countActivePurgeLocks() {
        return getJdbcTemplate().queryForInt(
                "select count(*) from sym_lock where lock_id=? and lock_action=? and lock_time is not null",
                new Object[] { ClusterConstants.COMMON_LOCK_ID, ClusterConstants.PURGE_INCOMING });
    }
}
