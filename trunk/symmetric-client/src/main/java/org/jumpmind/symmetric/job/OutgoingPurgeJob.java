
package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for purging already synchronized data
 */
public class OutgoingPurgeJob extends AbstractJob {

    public OutgoingPurgeJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.purge.outgoing", true, engine.getParameterService().is("start.purge.job"),
                engine, taskScheduler);
    }
    @Override
    public void doJob(boolean force) throws Exception {
        engine.getPurgeService().purgeOutgoing(force);        
    }
    
    public String getClusterLockName() {
        return ClusterConstants.PURGE_OUTGOING;
    }
    
    public boolean isClusterable() {
        return true;
    }
    
}