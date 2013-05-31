package org.jumpmind.symmetric.job;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;


/*
 * This job calls {@link IRouterService#routeData()} 
 */
public class RouterJob extends AbstractJob {
    
    public RouterJob(ISymmetricEngine engine, ThreadPoolTaskScheduler taskScheduler) {
        super("job.routing", true, engine.getParameterService().is("start.route.job"),
                engine, taskScheduler);
    }
    
    @Override
    void doJob(boolean force) throws Exception {
        engine.getRouterService().routeData(force);
    }

    public String getClusterLockName() {
        return ClusterConstants.ROUTE;
    }
    
    public boolean isClusterable() {
        return true;
    }
}