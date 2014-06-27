package org.jumpmind.symmetric.fs.client;

import org.jumpmind.symmetric.fs.config.Node;

public class NoOpServerNodeLocker implements IServerNodeLocker {

    public boolean lock(Node serverNode) {
        return true;
    }

    public boolean unlock(Node serverNode) {
        return true;
    }

}
