package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

public interface IEnvironment {

    public IResourceFactory getResourceFactory();
    
    public IDbPlatform getDbPlatform();
    
}
