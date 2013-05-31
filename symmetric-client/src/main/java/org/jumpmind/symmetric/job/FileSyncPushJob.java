package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class FileSyncPushJob extends AbstractJob {

    protected FileSyncPushJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.file.sync.push", true, engine.getParameterService().is(
                ParameterConstants.FILE_SYNC_ENABLE)
                && engine.getParameterService().is("start.file.sync.push.job", true), engine,
                taskScheduler);
    }

    public String getClusterLockName() {
        return ClusterConstants.FILE_SYNC_PUSH;
    }

    public boolean isClusterable() {
        return true;
    }

    @Override
    void doJob(boolean force) throws Exception {
        engine.getFileSyncService().pushFilesToNodes(force);
    }

}
