package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that is responsible for purging already synchronized data
 */
public class DataGapPurgeJob extends AbstractJob {

    public DataGapPurgeJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.purge.datagaps", true, engine.getParameterService().is("start.purge.job"),
                engine, taskScheduler);
    }

    @Override
    public void doJob(boolean force) throws Exception {
        engine.getPurgeService().purgeDataGaps(force);
    }

    public String getClusterLockName() {
        return ClusterConstants.PURGE_DATA_GAPS;
    }

    public boolean isClusterable() {
        return true;
    }

}