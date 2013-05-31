package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that pulls data from remote nodes and then loads it.
 */
public class PullJob extends AbstractJob {

    public PullJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.pull", false, engine.getParameterService().is("start.pull.job"),
                engine, taskScheduler);
    }
    
    @Override
    public void doJob(boolean force) throws Exception {
        engine.getPullService().pullData(force);
    }
    
    public String getClusterLockName() {
        return ClusterConstants.PULL;
    }
    
    public boolean isClusterable() {
        return true;
    }

}
