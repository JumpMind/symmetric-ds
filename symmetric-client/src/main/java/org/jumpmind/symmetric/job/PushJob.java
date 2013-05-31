package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for pushing data to linked nodes.
 */
public class PushJob extends AbstractJob {

    public PushJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.push", true, engine.getParameterService().is("start.push.job"), engine,
                taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        if (engine != null) {
            engine.getPushService().pushData(force).getDataProcessedCount();
        }
    }

    public String getClusterLockName() {
        return ClusterConstants.PUSH;
    }

    public boolean isClusterable() {
        return true;
    }

}