package org.jumpmind.symmetric.job;

import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IBootstrapService;

public class HeartbeatJob extends TimerTask {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IBootstrapService bootstrapService;

    @Override
    public void run() {
        try {
            bootstrapService.heartbeat();
        } catch (Throwable ex) {
            logger.error(ex, ex);
        }
    }

    public void setBootstrapService(IBootstrapService bootstrapService) {
        this.bootstrapService = bootstrapService;
    }

}