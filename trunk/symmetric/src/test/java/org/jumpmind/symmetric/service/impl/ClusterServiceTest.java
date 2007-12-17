package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.LockAction;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterServiceTest extends AbstractDatabaseTest {

    @Test(groups = "continuous")
    public void testLock() throws Exception {
        IClusterService service = (IClusterService) getBeanFactory().getBean(Constants.CLUSTER_SERVICE);
        Assert.assertTrue(service.lock(LockAction.PURGE), "Could not lock for PURGE");
        Assert.assertEquals(countActivePurgeLocks(), 1, "Could not find the lock in the database.");
        Assert.assertFalse(service.lock(LockAction.PURGE), "Should not have been able to lock for PURGE");
        service.unlock(LockAction.PURGE);
        Assert.assertEquals(countActivePurgeLocks(), 0, "Could not find the lock in the database.");
    }

    private int countActivePurgeLocks() {
        return getJdbcTemplate().queryForInt(
                "select count(*) from sym_lock where lock_id=? and lock_action=? and lock_time is not null",
                new Object[] { ClusterService.COMMON_LOCK_ID, LockAction.PURGE.name() });
    }
}
