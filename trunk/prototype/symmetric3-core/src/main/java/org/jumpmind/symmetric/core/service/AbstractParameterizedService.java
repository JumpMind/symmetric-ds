package org.jumpmind.symmetric.core.service;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.model.Parameters;

abstract public class AbstractParameterizedService extends AbstractService {

    protected ParameterService parameterService;

    public AbstractParameterizedService(IEnvironment environment, ParameterService parameterService) {
        super(environment);
        this.parameterService = parameterService;
    }
    
    protected Parameters getParameters() {
        return parameterService.getParameters();
    }

}
