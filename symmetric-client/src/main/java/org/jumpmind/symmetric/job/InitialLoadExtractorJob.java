package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

public class InitialLoadExtractorJob extends AbstractJob {

    protected InitialLoadExtractorJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.initial.load.extract", true, engine.getParameterService().is(
                "start.initial.load.extract.job", true), engine, taskScheduler);
    }

    public String getClusterLockName() {
        return ClusterConstants.INITIAL_LOAD_EXTRACT;
    }

    @Override
    void doJob(boolean force) throws Exception {
        engine.getDataExtractorService().queueWork(force);
    }

}
