package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class FileSyncPullJob extends AbstractJob {

    protected FileSyncPullJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.file.sync.pull", true, engine.getParameterService().is(
                ParameterConstants.FILE_SYNC_ENABLE) && engine.getParameterService().is("start.file.sync.pull.job", true), engine, taskScheduler);
    }

    public String getClusterLockName() {
        return ClusterConstants.FILE_SYNC_PULL;
    }

    public boolean isClusterable() {
        return true;
    }

    @Override
    void doJob(boolean force) throws Exception {
        engine.getFileSyncService().pullFilesFromNodes(force);
    }


}
