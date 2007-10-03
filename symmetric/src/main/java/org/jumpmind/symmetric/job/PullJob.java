package org.jumpmind.symmetric.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPullService;

public class PullJob extends AbstractJob {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IPullService pullService;

    @Override
    public void doJob() throws Exception {
        pullService.pullData();
    }

    public void setPullService(IPullService service) {
        this.pullService = service;
    }

    @Override
    Log getLogger() {
        return logger;
    }
}