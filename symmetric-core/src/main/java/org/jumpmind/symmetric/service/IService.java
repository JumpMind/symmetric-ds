package org.jumpmind.symmetric.service;


/**
 * An interface that indicates that this class provides stateless services methods for the application
 */
public interface IService {

    /**
     * Provide a mechanism where service clients may synchronize custom 
     * code with synchronized code in SymmetricDS services.
     * @param runnable The code to run
     */
    public void synchronize(Runnable runnable);
    
    public String getSql(String... keys);
    
}