package org.jumpmind.symmetric.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPurgeService;

public class PurgeJob extends AbstractJob {

    private static final Log logger = LogFactory.getLog(PurgeJob.class);

    private IPurgeService purgeService;

    public PurgeJob() {
    }

    @Override
    public void doJob() throws Exception {
        purgeService.purge();
    }

    public void setPurgeService(IPurgeService service) {
        this.purgeService = service;
    }

    @Override
    Log getLogger() {
        return logger;
    }
}
