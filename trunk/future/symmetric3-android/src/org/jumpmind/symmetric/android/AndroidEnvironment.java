package org.jumpmind.symmetric.android;

import org.jumpmind.symmetric.core.IEnvironment;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.resources.IResourceFactory;

public class AndroidEnvironment implements IEnvironment {

    IResourceFactory resourceFactory = new AndroidResourceFactory();    
    
    public IDbDialect getDbDialect() {
        // TODO Auto-generated method stub
        return null;
    }
    
    public IResourceFactory getResourceFactory() {
        return resourceFactory;
    }
    
}
