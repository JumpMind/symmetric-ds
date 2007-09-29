package org.jumpmind.symmetric.job;

import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPushService;

public class PushJob extends TimerTask {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IPushService pushService;

    public PushJob() {
    }

    public void setPushService(IPushService service) {
        this.pushService = service;
    }

    @Override
    public void run() {
        try {
            pushService.pushData();
        } catch (Throwable ex) {
            logger.error(ex, ex);
        }
    }

}
