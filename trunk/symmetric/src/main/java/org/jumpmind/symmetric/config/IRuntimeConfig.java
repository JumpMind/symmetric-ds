package org.jumpmind.symmetric.config;


/**
 * This interface is used to pull the runtime configuration for this
 * symmetric installation.
 * 
 * If the registrationURL is null, then this server will not register with another
 * server (it is likely that it is the host itself).
 * 
 * This interface is meant to be 'pluggable.'  It might be that different installations
 * might want to pull this information from different places.
 */
public interface IRuntimeConfig {

    /**
     * Get the group id for this instance
     */
    public String getNodeGroupId();

    /**
     * Get the external id for this instance
     */
    public String getExternalId();

    /**
     * Provide the url used to register at to get initial configuration information
     */
    public String getRegistrationUrl();
    
    /**
     * Provide information about the url used to contact this symmetric instance
     */
    public String getMyUrl();
    
    /**
     * Provide information about the version of the schema being sync'd.
     */
    public String getSchemaVersion();

}
