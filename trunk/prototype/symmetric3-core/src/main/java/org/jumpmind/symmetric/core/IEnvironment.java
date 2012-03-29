package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

public interface IEnvironment {

    public IResourceFactory getResourceFactory();

    public IDbDialect getDbDialect();
    
    /**
     * Get parameters that are specific to the environment
     */
    public Parameters getLocalParameters();

}
