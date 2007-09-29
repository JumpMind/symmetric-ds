package org.jumpmind.symmetric.service;

/**
 * Provides methods to setup the runtime for data synchronization based on {@link IConfigurationService}.
 */
public interface IBootstrapService {

    public void init();

    public void syncTriggers();
    
    public void register();
    
    public void heartbeat();

}
