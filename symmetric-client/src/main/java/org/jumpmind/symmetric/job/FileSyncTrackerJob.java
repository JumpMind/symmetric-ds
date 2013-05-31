package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class FileSyncTrackerJob extends AbstractJob {

    protected FileSyncTrackerJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.file.sync.tracker", true, engine.getParameterService().is(
                ParameterConstants.FILE_SYNC_ENABLE)
                && engine.getParameterService().is("start.file.sync.tracker.job", true), engine, taskScheduler);
    }

    public String getClusterLockName() {
        return ClusterConstants.FILE_SYNC_TRACKER;
    }

    public boolean isClusterable() {
        return true;
    }

    @Override
    void doJob(boolean force) throws Exception {
        if (engine != null) {
            engine.getFileSyncService().trackChanges(force);
        }
    }

}
