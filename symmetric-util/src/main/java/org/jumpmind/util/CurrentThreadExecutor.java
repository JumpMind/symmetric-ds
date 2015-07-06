package org.jumpmind.util;

import java.util.concurrent.Executor;

public class CurrentThreadExecutor implements Executor {
    public void execute(Runnable r) {
        r.run();
    }
}