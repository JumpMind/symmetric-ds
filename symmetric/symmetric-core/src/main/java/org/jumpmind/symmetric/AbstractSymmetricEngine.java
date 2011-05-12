/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */
package org.jumpmind.symmetric;

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.DeploymentType;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.config.PropertiesFactoryBean;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IExtensionPointManager;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.IUpgradeService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

public abstract class AbstractSymmetricEngine implements ISymmetricEngine {

    protected final ILog log = LogFactory.getLog(getClass());

    private static Map<String, ISymmetricEngine> registeredEnginesByUrl = new HashMap<String, ISymmetricEngine>();
    private static Map<String, ISymmetricEngine> registeredEnginesByName = new HashMap<String, ISymmetricEngine>();

    private ApplicationContext applicationContext;
    private JdbcTemplate jdbcTemplate;
    private boolean started = false;
    private boolean starting = false;
    private boolean setup = false;
    private IDbDialect dbDialect;
    private IJobManager jobManager;    
    
    protected AbstractSymmetricEngine() {
    }

    /**
     * Locate a {@link StandaloneSymmetricEngine} in the same JVM
     */
    public static ISymmetricEngine findEngineByUrl(String url) {
        if (registeredEnginesByUrl != null && url != null) {
            return registeredEnginesByUrl.get(url);
        } else {
            return null;
        }
    }

    /**
     * Locate a {@link StandaloneSymmetricEngine} in the same JVM
     */
    public static ISymmetricEngine findEngineByName(String name) {
        if (registeredEnginesByName != null && name != null) {
            return registeredEnginesByName.get(name.toLowerCase());
        } else {
            return null;
        }
    }

/**
     * Locate the one and only registered {@link StandaloneSymmetricEngine}.  Use {@link #findEngineByName(String)} or
     * {@link #findEngineByUrl(String) if there is more than on engine registered.
     * @throws IllegalStateException This exception happens if more than one engine is 
     * registered  
     */
    public static ISymmetricEngine getEngine() {
        int numberOfEngines = registeredEnginesByName.size();
        if (numberOfEngines == 0) {
            return null;
        } else if (numberOfEngines > 1) {
            throw new IllegalStateException("More than one SymmetricEngine is currently registered");
        } else {
            return registeredEnginesByName.values().iterator().next();
        }
    }

    public synchronized boolean start() {

        setup();
        
        if (isConfigured()) {
            if (!starting && !started) {
                try {
                    starting = true;
                    Node node = getNodeService().findIdentity();
                    if (node != null) {
                        log.info("RegisteredNodeStarting", node.getNodeGroupId(), node.getNodeId(),
                                node.getExternalId());
                    } else {
                        log.info("UnregisteredNodeStarting",
                                getParameterService().getNodeGroupId(), getParameterService()
                                        .getExternalId());
                    }
                    getTriggerRouterService().syncTriggers();
                    heartbeat(false);
                    jobManager.startJobs();
                    log.info("SymmetricDSStarted", getParameterService().getString(
                            ParameterConstants.ENGINE_NAME), getParameterService().getExternalId(),
                            Version.version(), dbDialect.getName(), dbDialect.getVersion());
                    started = true;
                } finally {
                    starting = false;
                }
                
                return true;
            } else {
                return false;
            }
        } else {
            log.warn("SymmetricDSNotStarted");
            return false;
        }
    }

    public synchronized void stop() {
        log.info("SymmetricDSClosing", getParameterService().getExternalId(), Version.version(), dbDialect.getName());
        jobManager.stopJobs();
        getRouterService().stop();
        started = false;
        starting = false;
    }
    
    public synchronized void destroy() {
        stop();
        jobManager.destroy();
        getRouterService().destroy();
        removeMeFromMap(registeredEnginesByName);
        removeMeFromMap(registeredEnginesByUrl);
        DataSource ds = jdbcTemplate.getDataSource();
        if (ds instanceof BasicDataSource) {
            try {
                ((BasicDataSource) ds).close();
            } catch (SQLException ex) {
                log.error(ex);
            }
        }

        applicationContext = null;
        jdbcTemplate = null;
        dbDialect = null;        
    }

    /**
     * @param overridePropertiesResource1
     *            Provide a Spring resource path to a properties file to be used for configuration
     * @param overridePropertiesResource2
     *            Provide a Spring resource path to a properties file to be used for configuration
     */
    protected void init(ApplicationContext ctx, boolean isParentContext, Properties overrideProperties,
            String overridePropertiesResource1, String overridePropertiesResource2) {
        // Setting system properties is probably not the best way to accomplish
        // this setup. Synchronizing on the class so creating multiple engines
        // is thread safe.
        synchronized (this.getClass()) {
            try {
                if (overrideProperties != null) {
                    PropertiesFactoryBean.setLocalProperties(overrideProperties);
                }

                if (!StringUtils.isBlank(overridePropertiesResource1)) {
                    System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1,
                            overridePropertiesResource1);
                }

                if (!StringUtils.isBlank(overridePropertiesResource2)) {
                    System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_2,
                            overridePropertiesResource2);
                }

                if (isParentContext || ctx == null) {
                    init(createContext(ctx));
                } else {
                    init(ctx);
                }
            } finally {
                PropertiesFactoryBean.clearLocalProperties();
            }
        }
    }

    protected void init(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        dbDialect = AppUtils.find(Constants.DB_DIALECT, this);
        jobManager = AppUtils.find(Constants.JOB_MANAGER, this);
        jdbcTemplate = AppUtils.find(Constants.JDBC_TEMPLATE, this);
        
        IExtensionPointManager extMgr = (IExtensionPointManager)this.applicationContext.getBean(Constants.EXTENSION_MANAGER);
        extMgr.register(); 
        
        getDeploymentType().setEngineRegistered(true);
    }
    
    protected abstract ApplicationContext createContext(ApplicationContext parentContext);

    private void removeMeFromMap(Map<String, ISymmetricEngine> map) {
        Set<String> keys = new HashSet<String>(map.keySet());
        for (String key : keys) {
            if (map.get(key).equals(this)) {
                map.remove(key);
            }
        }
    }

    /**
     * Register this instance of the engine so it can be found by other processes in the JVM.
     * 
     * @see #findEngineByUrl(String)
     */
    private void registerEngine() {
        String url = getSyncUrl();
        ISymmetricEngine alreadyRegister = registeredEnginesByUrl.get(url);
        if (alreadyRegister == null || alreadyRegister.equals(this)) {
            if (url != null) {
                registeredEnginesByUrl.put(url, this);
            }
        } else {
            throw new IllegalStateException("Could not register engine.  There was already an engine registered under the url: " + getSyncUrl());
        }
        
        alreadyRegister = registeredEnginesByName.get(getEngineName());
        if (alreadyRegister == null || alreadyRegister.equals(this)) {
            registeredEnginesByName.put(getEngineName(), this);
        } else {
            throw new IllegalStateException("Could not register engine.  There was already an engine registered under the name: " + getEngineName());
        }
        
        
    }

    public String getSyncUrl() {
        Node node = getNodeService().findIdentity();
        if (node != null) {
            return node.getSyncUrl();
        } else {
            return getParameterService().getSyncUrl();
        }
    }

    public Properties getProperties() {
        Properties p = new Properties();
        p.putAll(getParameterService().getAllParameters());
        return p;
    }

    public String getEngineName() {
        return dbDialect.getEngineName();
    }

    public synchronized void setup() {
        AppUtils.cleanupTempFiles();
        getParameterService().rereadParameters();
        if (!setup) {
            setupDatabase(false);
            setup = true;
        }
        registerEngine();
    }

    public String reloadNode(String nodeId) {
        return getDataService().reloadNode(nodeId);
    }

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName, String sql) {
        return getDataService().sendSQL(nodeId, catalogName, schemaName, tableName, sql, false);
    }

    public RemoteNodeStatuses push() {
        return getPushService().pushData();
    }

    public void syncTriggers() {
        getTriggerRouterService().syncTriggers();
    }

    public NodeStatus getNodeStatus() {
        return getNodeService().getNodeStatus();
    }

    public RemoteNodeStatuses pull() {
        return getPullService().pullData();
    }
    
    public void route() {
       getRouterService().routeData();        
    }

    public void purge() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(getParameterService().getString(ParameterConstants.START_PURGE_JOB))) {
            getPurgeService().purgeOutgoing();
        } else {
            throw new UnsupportedOperationException("Cannot actuate a purge if it is already scheduled.");
        }
    }

    protected void setupDatabase(boolean force) {
        getConfigurationService().autoConfigDatabase(force);
        if (getUpgradeService().isUpgradeNecessary()) {
            if (getParameterService().is(ParameterConstants.AUTO_UPGRADE)) {
                try {
                    if (getUpgradeService().isUpgradePossible()) {
                        getUpgradeService().upgrade();
                        // rerun the auto configuration to make sure things are
                        // kosher after the upgrade
                        getConfigurationService().autoConfigDatabase(force);
                    } else {
                        throw new SymmetricException("SymmetricDSManualUpgradeNeeded", getNodeService()
                                .findSymmetricVersion(), Version.version());
                    }
                } catch (RuntimeException ex) {
                    log.fatal("SymmetricDSUpgradeFailed", ex);
                    throw ex;
                }
            } else {
                throw new SymmetricException("SymmetricDSUpgradeNeeded");
            }
        }

        // lets do this every time init is called.
        getClusterService().initLockTable();
    }

    public boolean isConfigured() {
        boolean configurationValid = false;
        
        IDbDialect dbDialect = getDbDialect();
        
        boolean isRegistrationServer = getNodeService().isRegistrationServer();
        
        boolean isSelfConfigurable = isRegistrationServer && 
        (getParameterService().is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false) || 
         StringUtils.isNotBlank(getParameterService().getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT))); 
        
        Table symNodeTable = dbDialect.getTable(dbDialect.getDefaultCatalog(), dbDialect.getDefaultSchema(), TableConstants.getTableName(dbDialect.getTablePrefix(), TableConstants.SYM_NODE), false);
        
        Node node = symNodeTable != null ? getNodeService().findIdentity() : null;
        
        long offlineNodeDetectionPeriodSeconds = getParameterService().getLong(
                ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES) * 60;
        long heartbeatSeconds = getParameterService().getLong(
                ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);
        
        String registrationUrl = getParameterService().getRegistrationUrl();
        
        if (!isSelfConfigurable && node == null && isRegistrationServer) {
            log.warn("ValidationRegServerIsMissingConfiguration", ParameterConstants.REGISTRATION_URL);
        } else if (!isSelfConfigurable && node == null && StringUtils.isBlank(getParameterService().getRegistrationUrl())) {
            log.warn("ValidationSetRegistrationUrl", ParameterConstants.REGISTRATION_URL);            
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getExternalId()) || 
                Constants.PLEASE_SET_ME.equals(registrationUrl) || 
                Constants.PLEASE_SET_ME.equals(getParameterService().getNodeGroupId())) {
            log.warn("ValidationPleaseSetMe");            
        } else if (node != null
                && (!node.getExternalId().equals(getParameterService().getExternalId()) || !node
                        .getNodeGroupId().equals(getParameterService().getNodeGroupId()))) {
            log.warn("ValidationComparePropertiesToDatabase", node.getExternalId(),
                    getParameterService().getExternalId(), node.getNodeGroupId(),
                    getParameterService().getNodeGroupId());
            
        } else if (node != null && StringUtils.isBlank(getParameterService().getRegistrationUrl())
                && StringUtils.isBlank(getParameterService().getSyncUrl())) {
            log.warn("ValidationMakeSureSyncUrlIsSet");
            
        } else if (offlineNodeDetectionPeriodSeconds > 0
                && offlineNodeDetectionPeriodSeconds <= heartbeatSeconds) {
            // Offline node detection is not disabled (-1) and the value is too
            // small (less than the heartbeat)
            log.warn("ValidationOfflineSettings",
                    ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES,
                    ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);
            
        } else {
            configurationValid = true;
        }
        
        
        // TODO Add more validation checks to make sure that the system is
        // configured correctly

        // TODO Add method to configuration service to validate triggers and
        // call from here.
        // Make sure there are not duplicate trigger rows with the same name
        
        return configurationValid;
    }

    public void heartbeat(boolean force) {
        getDataService().heartbeat(force);
    }

    public void openRegistration(String groupId, String externalId) {
        getRegistrationService().openRegistration(groupId, externalId);
    }

    public void reOpenRegistration(String nodeId) {
        getRegistrationService().reOpenRegistration(nodeId);
    }

    public boolean isRegistered() {
        return getNodeService().findIdentity() != null;
    }

    public boolean isStarted() {
        return started;
    }

    public boolean isStarting() {
        return starting;
    }

    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    public IConfigurationService getConfigurationService() {
        return AppUtils.find(Constants.CONFIG_SERVICE, this);
    }

    public IParameterService getParameterService() {
        return AppUtils.find(Constants.PARAMETER_SERVICE, this);
    }

    public INodeService getNodeService() {
        return AppUtils.find(Constants.NODE_SERVICE, this);
    }
    
    public DeploymentType getDeploymentType() {
        return AppUtils.find(Constants.DEPLOYMENT_TYPE, this);
    }

    public IRegistrationService getRegistrationService() {
        return AppUtils.find(Constants.REGISTRATION_SERVICE, this);
    }

    public IUpgradeService getUpgradeService() {
        return AppUtils.find(Constants.UPGRADE_SERVICE, this);
    }

    public IClusterService getClusterService() {
        return AppUtils.find(Constants.CLUSTER_SERVICE, this);
    }

    public IPurgeService getPurgeService() {
        return AppUtils.find(Constants.PURGE_SERVICE, this);
    }

    public IDataService getDataService() {
        return AppUtils.find(Constants.DATA_SERVICE, this);
    }

    public IDbDialect getDbDialect() {
        return dbDialect;
    }

    public IJobManager getJobManager() {
        return jobManager;
    }    
    
    public IOutgoingBatchService getOutgoingBatchService() {
    	return AppUtils.find(Constants.OUTGOING_BATCH_SERVICE, this);
    }
    
    public IAcknowledgeService getAcknowledgeService() {
    	return AppUtils.find(Constants.ACKNOWLEDGE_SERVICE, this);
    }
    
    public IBandwidthService getBandwidthService() {
    	return AppUtils.find(Constants.BANDWIDTH_SERVICE, this);
    }
    
    public IDataExtractorService getDataExtractorService() {
    	return AppUtils.find(Constants.DATAEXTRACTOR_SERVICE, this);
    }
    
    public IDataLoaderService getDataLoaderService() {
    	return AppUtils.find(Constants.DATALOADER_SERVICE, this);
    }
    
    public IIncomingBatchService getIncomingBatchService() {
    	return AppUtils.find(Constants.INCOMING_BATCH_SERVICE, this);
    }
    
    public IPullService getPullService() {
    	return AppUtils.find(Constants.PULL_SERVICE, this);
    }
    
    public IPushService getPushService() {
    	return AppUtils.find(Constants.PUSH_SERVICE, this);
    }
    
    public IRouterService getRouterService() {
    	return AppUtils.find(Constants.ROUTER_SERVICE, this);
    }
    
    public ISecurityService getSecurityService() {
    	return AppUtils.find(Constants.SECURITY_SERVICE, this);
    }
    
    public IStatisticManager getStatisticManager() {
        return AppUtils.find(Constants.STATISTIC_MANAGER, this);
    }
    
    public IStatisticService getStatisticService() {
    	return AppUtils.find(Constants.STATISTIC_SERVICE, this);
    }
    
    public ITriggerRouterService getTriggerRouterService() {
    	return AppUtils.find(Constants.TRIGGER_ROUTER_SERVICE, this);
    }
    
    public DataSource getDataSource() {
        return AppUtils.find(Constants.DATA_SOURCE, this);
    }

}