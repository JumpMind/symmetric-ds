package org.jumpmind.symmetric.web;

import java.io.File;
import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.ClientSymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;

public class ServerSymmetricEngine extends ClientSymmetricEngine {

    protected List<IUriHandler> uriHandlers;

    public ServerSymmetricEngine(File propertiesFile) {
        super(propertiesFile);
    }

    @Override
    protected void init() {
        super.init();

        AuthenticationInterceptor authInterceptor = new AuthenticationInterceptor(nodeService);
        NodeConcurrencyInterceptor concurrencyInterceptor = new NodeConcurrencyInterceptor(
                concurrentConnectionManager, configurationService, statisticManager);

        this.uriHandlers = new ArrayList<IUriHandler>();
        this.uriHandlers.add(new AckUriHandler(log, parameterService, acknowledgeService,
                authInterceptor));
        this.uriHandlers.add(new PingUriHandler(log, parameterService));
        this.uriHandlers.add(new InfoUriHandler(log, parameterService, nodeService,
                configurationService));
        this.uriHandlers.add(new BandwidthSamplerUriHandler(log, parameterService));
        this.uriHandlers.add(new PullUriHandler(log, parameterService, nodeService,
                configurationService, dataExtractorService, registrationService, statisticManager,
                concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new PushUriHandler(log, parameterService, dataLoaderService,
                statisticManager, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new RegistrationUriHandler(log, parameterService, registrationService,
                concurrencyInterceptor));
        if (parameterService.is(ParameterConstants.WEB_BATCH_URI_HANDLER_ENABLE)) {
            this.uriHandlers.add(new BatchUriHandler(log, parameterService, dataExtractorService));
        }
    }

    public List<IUriHandler> getUriHandlers() {
        return uriHandlers;
    }

}
