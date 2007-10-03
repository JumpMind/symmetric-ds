package org.jumpmind.symmetric.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBootstrapService;

public class SyncTriggersJob extends AbstractJob {

    private static final Log logger = LogFactory.getLog(SyncTriggersJob.class);

    private IBootstrapService bootstrapService;

    public SyncTriggersJob() {
    }

    @Override
    public void doJob() throws Exception {
        bootstrapService.syncTriggers();
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

    @Override
    Log getLogger() {
        return logger;
    }
}
