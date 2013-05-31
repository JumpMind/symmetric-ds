package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for checking on node health. It will
 * disable nodes that have been offline for a configurable period of time.
 */
public class WatchdogJob extends AbstractJob {
    
    public WatchdogJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.watchdog", false, engine.getParameterService().is("start.watchdog.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        if (engine.getClusterService().lock(ClusterConstants.WATCHDOG)) {
            synchronized (this) {
                try {
                    engine.getNodeService().checkForOfflineNodes();
                } finally {
                    engine.getClusterService().unlock(ClusterConstants.WATCHDOG);
                }
            }
        }
    }

    public String getClusterLockName() {
        return ClusterConstants.WATCHDOG;
    }

    public boolean isClusterable() {
        return true;
    }
}