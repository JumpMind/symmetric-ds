package org.jumpmind.symmetric.job;

import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBootstrapService;

public class SyncTriggersJob extends TimerTask {

    private static final Log logger = LogFactory.getLog(SyncTriggersJob.class);

    private IBootstrapService bootstrapService;

    public SyncTriggersJob() {
    }

    @Override
    public void run() {
        try {
            bootstrapService.syncTriggers();
        } catch (Throwable ex) {
            logger.error(ex, ex);
        }
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

}
