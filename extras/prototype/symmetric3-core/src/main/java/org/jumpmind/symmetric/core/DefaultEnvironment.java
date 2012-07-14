package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

public class DefaultEnvironment implements IEnvironment {

    protected IResourceFactory resourceFactory;
    protected IDbDialect dbDialect;
    protected Parameters localParameters;

    public DefaultEnvironment(IResourceFactory resourceFactory, IDbDialect dbDialect, Parameters parameters) {
        this.resourceFactory = resourceFactory;
        this.dbDialect = dbDialect;
        this.localParameters = parameters;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public IDbDialect getDbDialect() {
        return dbDialect;
    }

    public void setResourceFactory(IResourceFactory resourceFactory) {
        this.resourceFactory = resourceFactory;
    }

    public IResourceFactory getResourceFactory() {
        return resourceFactory;
    }
    
    public Parameters getLocalParameters() {
        return localParameters;
    }

}
