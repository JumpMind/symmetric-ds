package org.jumpmind.symmetric.job;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.service.IPushService;

public class PushJob extends AbstractJob {

    private static final Log logger = LogFactory.getLog(PushJob.class);

    private IPushService pushService;

    public PushJob() {
    }

    public void setPushService(IPushService service) {
        this.pushService = service;
    }

    @Override
    public void doJob() throws Exception {
        pushService.pushData();
    }

    @Override
    Log getLogger() {
        return logger;
    }
}
