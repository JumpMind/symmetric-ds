package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.core.IEnvironment;

public class TriggerRouterService extends AbstractParameterizedService {

    public TriggerRouterService(IEnvironment environment, ParameterService parameterSerivce) {
        super(environment, parameterSerivce);
    }
    
    public void syncTriggers() {}

}
