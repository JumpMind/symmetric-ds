
package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that checks to see if triggers need to be regenerated.
 */
public class SyncTriggersJob extends AbstractJob {

    public SyncTriggersJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.synctriggers", true, engine.getParameterService().is("start.synctriggers.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        engine.getTriggerRouterService().syncTriggers();
    }

    public String getClusterLockName() {
        return ClusterConstants.SYNCTRIGGERS;
    }
    
    public boolean isClusterable() {
        return true;
    }

}