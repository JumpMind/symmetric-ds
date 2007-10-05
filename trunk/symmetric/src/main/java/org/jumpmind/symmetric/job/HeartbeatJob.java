package org.jumpmind.symmetric.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBootstrapService;

public class HeartbeatJob extends AbstractJob {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IBootstrapService bootstrapService;

    @Override
    public void doJob() throws Exception {
        printDatabaseStats();
        bootstrapService.heartbeat();
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    @Override
    Log getLogger() {
        return logger;
    }

}