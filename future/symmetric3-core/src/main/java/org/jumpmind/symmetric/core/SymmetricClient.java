package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.core.service.ConfigurationService;
import org.jumpmind.symmetric.core.service.ParameterService;
import org.jumpmind.symmetric.core.service.TriggerRouterService;

public class SymmetricClient {

    final static Log log = LogFactory.getLog(SymmetricClient.class);

    protected IEnvironment environment;

    protected IDbDialect dbDialect;

    protected SymmetricTables symmetricDatabase;
    
    protected ParameterService parameterService;
    
    protected ConfigurationService configurationService;
    
    protected TriggerRouterService triggerRouterService;

    public SymmetricClient(IEnvironment environment) {
        this.environment = environment;
        this.dbDialect = this.environment.getDbDialect();
        this.symmetricDatabase = this.dbDialect.getSymmetricTables();
        this.parameterService = new ParameterService(environment);
        this.configurationService = new ConfigurationService(environment, this.parameterService);
        this.triggerRouterService = new TriggerRouterService(environment, this.parameterService);
    }

    public void initialize() {
        this.configurationService.autoConfigTables();
        this.configurationService.autoConfigFunctions();
        this.configurationService.autoConfigChannels();
        this.configurationService.autoConfigRegistrationServer();
        this.triggerRouterService.syncTriggers();
    }

    public void syncTriggers() {
        this.triggerRouterService.syncTriggers();
    }

    public RemoteNodeStatuses push() {
        return null;
    }

    public RemoteNodeStatuses pull() {
        return null;
    }



}
