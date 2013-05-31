
package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for purging already synchronized data
 */
public class IncomingPurgeJob extends AbstractJob {

    public IncomingPurgeJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.purge.incoming", true, engine.getParameterService().is("start.purge.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        engine.getPurgeService().purgeIncoming(force);  
    }
    
    public String getClusterLockName() {
        return ClusterConstants.PURGE_INCOMING;
    }
    
    public boolean isClusterable() {
        return true;
    }
    
}