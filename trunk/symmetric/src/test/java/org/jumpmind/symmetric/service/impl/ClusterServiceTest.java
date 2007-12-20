
package org.jumpmind.symmetric.service.impl;

import org.jumpmind.symmetric.AbstractDatabaseTest;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.LockAction;
import org.testng.Assert;
import org.testng.annotations.Test;

public class ClusterServiceTest extends AbstractDatabaseTest
{

    @Test(groups = "continuous")
    public void testLock() throws Exception
    {
        final IClusterService service = (IClusterService) getBeanFactory().getBean(Constants.CLUSTER_SERVICE);
        Assert.assertTrue(service.lock(LockAction.PURGE), "Could not lock for PURGE");
        Assert.assertEquals(countActivePurgeLocks(), 1, "Could not find the lock in the database.");
        Assert.assertFalse(service.lock(LockAction.PURGE), "Should not have been able to lock for PURGE");
        service.unlock(LockAction.PURGE);
        Assert.assertEquals(countActivePurgeLocks(), 0, "Could not find the lock in the database.");
    }

    @Test(groups = "continuous")
    public void testOtherNodeLock() throws Exception
    {
        final String ID_ONE = "00020";
        final String ID_TWO = "00010";
        final Node nodeOne = new Node();
        nodeOne.setNodeId(ID_ONE);

        final Node nodeTwo = new Node();
        nodeTwo.setNodeId(ID_TWO);

        final IClusterService service = (IClusterService) getBeanFactory().getBean(Constants.CLUSTER_SERVICE);
        service.initLockTable(LockAction.OTHER, ID_ONE);
        service.initLockTable(LockAction.OTHER, ID_TWO);
        Assert.assertTrue(service.lock(LockAction.OTHER, nodeOne), "Could not lock for OTHER " + ID_ONE);
        Assert.assertFalse(service.lock(LockAction.OTHER, nodeOne), "Should not have been able to lock for OTHER "
            + ID_ONE);
        Assert.assertTrue(service.lock(LockAction.OTHER, nodeTwo), "Could not lock for OTHER " + ID_TWO);
        service.unlock(LockAction.OTHER, nodeOne);
        Assert.assertTrue(service.lock(LockAction.OTHER, nodeOne), "Could not lock for OTHER " + ID_ONE);
    }

    private int countActivePurgeLocks()
    {
        return getJdbcTemplate().queryForInt(
            "select count(*) from sym_lock where lock_id=? and lock_action=? and lock_time is not null",
            new Object[] {ClusterService.COMMON_LOCK_ID, LockAction.PURGE.name()});
    }
}
