package org.jumpmind.symmetric.job;

import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPullService;

public class PullJob extends TimerTask {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IPullService pullService;

    @Override
    public void run() {
        try {
            pullService.pullData();
        } catch (Throwable ex) {
            logger.error(ex, ex);
        }
    }

    public void setPullService(IPullService service) {
        this.pullService = service;
    }

}