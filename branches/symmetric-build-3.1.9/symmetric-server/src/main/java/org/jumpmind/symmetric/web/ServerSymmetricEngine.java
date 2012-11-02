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
        this.uriHandlers.add(new AckUriHandler(parameterService, acknowledgeService,
                authInterceptor));
        this.uriHandlers.add(new PingUriHandler(parameterService));
        this.uriHandlers
                .add(new InfoUriHandler(parameterService, nodeService, configurationService));
        this.uriHandlers.add(new BandwidthSamplerUriHandler(parameterService));
        this.uriHandlers.add(new PullUriHandler(parameterService, nodeService,
                configurationService, dataExtractorService, registrationService, statisticManager,
                concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new PushUriHandler(parameterService, dataLoaderService,
                statisticManager, nodeService, concurrencyInterceptor, authInterceptor));
        this.uriHandlers.add(new RegistrationUriHandler(parameterService, registrationService,
                concurrencyInterceptor));
        if (parameterService.is(ParameterConstants.WEB_BATCH_URI_HANDLER_ENABLE)) {
            this.uriHandlers.add(new BatchUriHandler(parameterService, dataExtractorService));
        }
    }

    public List<IUriHandler> getUriHandlers() {
        return uriHandlers;
    }

}
