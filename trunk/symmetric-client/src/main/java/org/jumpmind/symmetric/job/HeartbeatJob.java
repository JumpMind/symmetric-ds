package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for updating this node's heart beat time.
 */
public class HeartbeatJob extends AbstractJob {

    public HeartbeatJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.heartbeat", false, engine.getParameterService().is("start.heartbeat.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        if (engine.getClusterService().lock(getClusterLockName())) {
            try {
                engine.getDataService().heartbeat(false);
            } finally {
                engine.getClusterService().unlock(getClusterLockName());
            }
        }
    }

    public String getClusterLockName() {
        return ClusterConstants.HEARTBEAT;
    }

    public boolean isClusterable() {
        return true;
    }

}