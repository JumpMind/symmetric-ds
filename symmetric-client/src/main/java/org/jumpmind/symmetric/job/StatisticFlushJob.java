
package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;


/*
 * Background job that is responsible for writing statistics to database tables.
 */
public class StatisticFlushJob extends AbstractJob {

    public StatisticFlushJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.stat.flush", true, engine.getParameterService().is("start.stat.flush.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        engine.getStatisticManager().flush();
        engine.getPurgeService().purgeStats(force);
    }
    
    public String getClusterLockName() {
        return ClusterConstants.STATISTICS;
    }
    
    public boolean isClusterable() {
        return false;
    }
}