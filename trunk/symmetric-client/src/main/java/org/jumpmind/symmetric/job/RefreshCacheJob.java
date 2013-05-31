package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/*
 * Background job that checks if cached objects should be refreshed
 */
public class RefreshCacheJob extends AbstractJob {

    public RefreshCacheJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.refresh.cache", false, engine.getParameterService().is("start.refresh.cache.job"),
                engine, taskScheduler);
    }
    
    @Override
    public void doJob(boolean force) throws Exception {
        engine.getParameterService().refreshFromDatabase();
        engine.getTriggerRouterService().refreshFromDatabase();
        engine.getGroupletService().refreshFromDatabase();
        engine.getConfigurationService().refreshFromDatabase();
        engine.getTransformService().refreshFromDatabase();
        engine.getDataLoaderService().refreshFromDatabase();
        engine.getLoadFilterService().refreshFromDatabase();
    }
    
    public String getClusterLockName() {
        return ClusterConstants.REFRESH_CACHE;
    }
    
    public boolean isClusterable() {
        return false;
    }

}
