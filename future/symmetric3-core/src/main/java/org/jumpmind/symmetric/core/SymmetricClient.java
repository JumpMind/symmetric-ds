package org.jumpmind.symmetric.core;

import org.jumpmind.symmetric.core.model.RemoteNodeStatuses;

public class SymmetricClient {

    protected IEnvironment environment;

    public SymmetricClient(IEnvironment environment) {
        this.environment = environment;
        initServices();
    }
    
    public void initialize() {
        initDatabase();
    }
    
    public void syncTriggers() {}
    
    public RemoteNodeStatuses push() {
        return null;
    }
    
    public RemoteNodeStatuses pull() {
        return null;
    }
    
    protected void initServices() {}
    
    protected void initDatabase() {}
    
}
