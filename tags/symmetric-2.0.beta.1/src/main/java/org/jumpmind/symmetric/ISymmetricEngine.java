package org.jumpmind.symmetric;

import java.util.Properties;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.job.PullJob;
import org.jumpmind.symmetric.job.PurgeJob;
import org.jumpmind.symmetric.job.PushJob;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.IUpgradeService;
import org.springframework.context.ApplicationContext;

public interface ISymmetricEngine {

    public void stop();

    /**
     * @return the URL that represents this engine
     */
    public String getMyUrl();

    /**
     * Get a list of configured properties for Symmetric. Read-only.
     */
    public Properties getProperties();

    /**
     * @return The lower case representation of the engine name as setup in the
     *         symmetric.properties file. We always use a lower case
     *         representation because there are times the engine name is used in
     *         triggers at which point you can lose the original case
     *         representation.
     */
    public String getEngineName();

    /**
     * Will setup the SymmetricDS tables, if not already setup and if the engine
     * is configured to do so.
     */
    public void setup();

    /**
     * Must be called to start SymmetricDS.
     */
    public void start();

    /**
     * Queue up an initial load or a reload to a node.
     */
    public String reloadNode(String nodeId);

    public String sendSQL(String nodeId, String tableName, String sql);

    /**
     * Will perform a push the same way the {@link PushJob} would have.
     * 
     * @see IPushService#pushData()
     * @return true if data was pushed successfully
     */
    public boolean push();

    /**
     * Call this to resync triggers
     * 
     * @see ITriggerRouterService#syncTriggers()
     */
    public void syncTriggers();

    /**
     * Get the current status of this node.
     * 
     * @return {@link NodeStatus}
     */
    public NodeStatus getNodeStatus();

    /**
     * Will perform a pull the same way the {@link PullJob} would have.
     * 
     * @see IPullService#pullData()
     */
    public boolean pull();

    /**
     * This can be called to do a purge. It may be called only if the
     * {@link PurgeJob} has not been enabled.
     * 
     * @see IPurgeService#purge()
     */
    public void purge();

    /**
     * Simply check and make sure that this node is all configured properly for
     * operation.
     */
    public void validateConfiguration();

    /**
     * Push a copy of the node onto the push queue so the SymmetricDS node
     * 'checks' in with it's root node.
     * 
     * @see IconfigurationService#heartbeat()
     */
    public void heartbeat(boolean force);

    /**
     * Open up registration for node to attach.
     * 
     * @see IRegistrationService#openRegistration(String, String)
     */
    public void openRegistration(String groupId, String externalId);

    public void reOpenRegistration(String nodeId);

    /**
     * Check to see if this node has been registered.
     * 
     * @return true if the node is registered
     */
    public boolean isRegistered();

    /**
     * Check to see if this node has been started.
     * 
     * @return true if the node is started
     */
    public boolean isStarted();

    /**
     * Check to see if this node is starting.
     * 
     * @return true if the node is starting
     */

    public boolean isStarting();

    /**
     * Will check to see if this instance of SymmetricDS is configured with the
     * required properties for a node to operate.
     */
    public boolean isConfigured();

    /**
     * Expose access to the Spring context. This is for advanced use only.
     * 
     * @return the Spring application context that SymmetricDS runs in
     */
    public ApplicationContext getApplicationContext();

    public IConfigurationService getConfigurationService();

    public IParameterService getParameterService();

    public INodeService getNodeService();

    public IRegistrationService getRegistrationService();

    public IUpgradeService getUpgradeService();

    public IClusterService getClusterService();

    public IPurgeService getPurgeService();

    public ITriggerRouterService getTriggerService();

    public IDataService getDataService();

    public IDbDialect getDbDialect();

    public IJobManager getJobManager();

}