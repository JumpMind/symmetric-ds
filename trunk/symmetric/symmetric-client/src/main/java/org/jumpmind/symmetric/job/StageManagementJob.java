package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class StageManagementJob extends AbstractJob {

    private IStagingManager stagingManager;

    public StageManagementJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler,
            IStagingManager stagingManager) {
        super("job.stage.management", true, engine.getParameterService().is(
                "start.stage.management.job"), engine, taskScheduler);
    }

    public String getClusterLockName() {
        return ClusterConstants.STAGE_MANAGEMENT;
    }

    public boolean isClusterable() {
        return true;
    }

    @Override
    long doJob() throws Exception {
        if (stagingManager != null) {
            long cleanupCount = stagingManager.clean();

            // TODO it would be a nice feature to be able to import from an
            // upload/import directory any files that are dropped there.

            return cleanupCount;
        } else {
            return 0;
        }
    }

}
