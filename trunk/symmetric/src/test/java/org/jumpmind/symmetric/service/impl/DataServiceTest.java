package org.jumpmind.symmetric.service.impl;

import java.util.List;
import java.util.Set;

import junit.framework.Assert;

import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.test.AbstractDatabaseTest;
import org.jumpmind.symmetric.util.AppUtils;
import org.junit.Test;

public class DataServiceTest extends AbstractDatabaseTest {

    public DataServiceTest() throws Exception {
        super();
    }

    public DataServiceTest(String dbType) {
        super(dbType);
    }

    @Test
    public void testGetHeartbeatListeners() throws Exception {
        DataService ds = new DataService();
        MockHeartbeatListener one = new MockHeartbeatListener();
        MockHeartbeatListener two = new MockHeartbeatListener();
        one.timeBetweenHeartbeats = 10000;
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
        one.timeBetweenHeartbeats = 100;
        AppUtils.sleep(100);
        listeners = ds.getHeartbeatListeners(false);
        Assert.assertEquals(2, listeners.size());
    }

    class MockHeartbeatListener implements IHeartbeatListener {
        protected boolean heartbeated = false;
        protected long timeBetweenHeartbeats;

        public long getTimeBetweenHeartbeats() {
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
