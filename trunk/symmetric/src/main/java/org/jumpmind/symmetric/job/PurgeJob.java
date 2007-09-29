package org.jumpmind.symmetric.job;

import java.util.TimerTask;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPurgeService;

public class PurgeJob extends TimerTask {

    private static final Log logger = LogFactory.getLog(PurgeJob.class);

    private IPurgeService purgeService;
    
    public PurgeJob() {
    }

    @Override
    public void run() {
        try {
            purgeService.purge();
        } catch (Throwable ex) {
            logger.error(ex, ex);
        }
    }

    public void setPurgeService(IPurgeService service)
    {
        this.purgeService = service;
    }

}
