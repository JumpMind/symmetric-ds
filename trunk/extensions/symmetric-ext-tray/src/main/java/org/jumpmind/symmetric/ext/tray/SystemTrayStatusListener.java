package org.jumpmind.symmetric.ext.tray;

import java.util.Set;

import org.jumpmind.symmetric.ext.IHeartbeatListener;
import org.jumpmind.symmetric.model.Node;

public class SystemTrayStatusListener implements IHeartbeatListener {

    @Override
    public long getTimeBetweenHeartbeatsInSeconds() {
        // TODO Auto-generated method stub
        return 0;
    }

    @Override
    public void heartbeat(Node me, Set<Node> children) {
        // TODO Auto-generated method stub

    }

    @Override
    public boolean isAutoRegister() {
        // TODO Auto-generated method stub
        return false;
    }

}
