package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

public class DefaultEnvironment implements IEnvironment {

    protected IResourceFactory resourceFactory;
    protected IDbPlatform dbPlatform;

    public DefaultEnvironment() {
    }

    public DefaultEnvironment(IResourceFactory resourceFactory, IDbPlatform dbPlatform) {
        this.resourceFactory = resourceFactory;
        this.dbPlatform = dbPlatform;
    }

    public void setDbPlatform(IDbPlatform dbPlatform) {
        this.dbPlatform = dbPlatform;
    }

    public IDbPlatform getDbPlatform() {
        return dbPlatform;
    }

    public void setResourceFactory(IResourceFactory resourceFactory) {
        this.resourceFactory = resourceFactory;
    }

    public IResourceFactory getResourceFactory() {
        return resourceFactory;
    }

}
