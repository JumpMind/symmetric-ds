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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.Node;
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
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

public abstract class AbstractSymmetricEngine implements ISymmetricEngine {

    protected final ILog log = LogFactory.getLog(getClass());

    private static Map<String, ISymmetricEngine> registeredEnginesByUrl = new HashMap<String, ISymmetricEngine>();
    private static Map<String, ISymmetricEngine> registeredEnginesByName = new HashMap<String, ISymmetricEngine>();
    
    private ApplicationContext applicationContext;
    private IConfigurationService configurationService;
    private IParameterService parameterService;
    private INodeService nodeService;
    private IRegistrationService registrationService;
    private IUpgradeService upgradeService;
    private IClusterService clusterService;
    private IPurgeService purgeService;
    private ITriggerRouterService triggerService;
    private IDataService dataService;
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
    
    public boolean isConfigured() {
        return parameterService != null && !StringUtils.isBlank(parameterService.getNodeGroupId()) &&
        !StringUtils.isBlank(parameterService.getExternalId()) && !StringUtils.isBlank(parameterService.getSyncUrl());
    }

    public synchronized void start() {
        if (!starting && !started) {
            try {
                starting = true;
                parameterService.rereadParameters();
                setup();
                validateConfiguration();
                registerEngine();
                startDefaultServerJMXExport();
                Node node = nodeService.findIdentity();
                if (node != null) {
                    log.info("RegisteredNodeStarting", node.getNodeGroupId(), node.getNodeId(), node.getExternalId());
                } else {
                    log.info("UnregisteredNodeStarting", parameterService.getNodeGroupId(), parameterService
                            .getExternalId());
                }
                triggerService.syncTriggers();
                heartbeat(false);
                jobManager.startJobs();
                log
                        .info("SymmetricDSStarted", parameterService.getExternalId(), Version.version(), dbDialect
                                .getName());
                started = true;
            } finally {
                starting = false;
            }
        }
    }

    public synchronized void stop() {
        log.info("SymmetricDSClosing", parameterService.getExternalId(), Version.version(), dbDialect.getName());
        jobManager.stopJobs();
        removeMeFromMap(registeredEnginesByName);
        removeMeFromMap(registeredEnginesByUrl);
        DataSource ds = dbDialect.getJdbcTemplate().getDataSource();
        if (ds instanceof BasicDataSource) {
            try {
                ((BasicDataSource) ds).close();
            } catch (SQLException ex) {
                log.error(ex);
            }
        }
        applicationContext = null;
        configurationService = null;
        parameterService = null;
        clusterService = null;
        upgradeService = null;
        triggerService = null;
        nodeService = null;
        registrationService = null;
        purgeService = null;
        dataService = null;
        dbDialect = null;
        started = false;
        starting = false;
    }

    /**
     * @param overridePropertiesResource1
     *            Provide a Spring resource path to a properties file to be used
     *            for configuration
     * @param overridePropertiesResource2
     *            Provide a Spring resource path to a properties file to be used
     *            for configuration
     */
    protected void init(ApplicationContext ctx, boolean isParentContext, Properties overrideProperties,
            String overridePropertiesResource1, String overridePropertiesResource2) {
        // Setting system properties is probably not the best way to accomplish
        // this setup. Synchronizing on the class so creating multiple engines
        // is thread safe.
        synchronized (StandaloneSymmetricEngine.class) {
            if (overrideProperties != null) {
                for (Object key : overrideProperties.keySet()) {
                    log.debug("InitAddingSystemProperty", key, overrideProperties.getProperty((String) key));
                }
                System.getProperties().putAll(overrideProperties);
            }
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, overridePropertiesResource1 == null ? ""
                    : overridePropertiesResource1);
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_2, overridePropertiesResource2 == null ? ""
                    : overridePropertiesResource2);
            if (isParentContext) {
                init(createContext(ctx));
            } else {
                init(createContext(null));
            }
        }
    }

    protected void init(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        configurationService = AppUtils.find(Constants.CONFIG_SERVICE, this);
        parameterService = AppUtils.find(Constants.PARAMETER_SERVICE, this);
        nodeService = AppUtils.find(Constants.NODE_SERVICE, this);
        registrationService = AppUtils.find(Constants.REGISTRATION_SERVICE, this);
        upgradeService = AppUtils.find(Constants.UPGRADE_SERVICE, this);
        clusterService = AppUtils.find(Constants.CLUSTER_SERVICE, this);
        purgeService = AppUtils.find(Constants.PURGE_SERVICE, this);
        dataService = AppUtils.find(Constants.DATA_SERVICE, this);
        triggerService = AppUtils.find(Constants.TRIGGER_ROUTER_SERVICE, this);
        dbDialect = AppUtils.find(Constants.DB_DIALECT, this);
        jobManager = AppUtils.find(Constants.JOB_MANAGER, this);
    }

    private ApplicationContext createContext(ApplicationContext parentContext) {
        return new ClassPathXmlApplicationContext(new String[] { "classpath:/symmetric.xml" }, parentContext);
    }

    private void removeMeFromMap(Map<String, ISymmetricEngine> map) {
        Set<String> keys = new HashSet<String>(map.keySet());
        for (String key : keys) {
            if (map.get(key).equals(this)) {
                map.remove(key);
            }
        }
    }

    /**
     * Register this instance of the engine so it can be found by other
     * processes in the JVM.
     * 
     * @see #findEngineByUrl(String)
     */
    private void registerEngine() {
        registeredEnginesByUrl.put(getMyUrl(), this);
        registeredEnginesByName.put(getEngineName(), this);
    }

    public String getMyUrl() {
        Node node = nodeService.findIdentity();
        if (node != null) {
            return node.getSyncURL();
        } else {
            return parameterService.getSyncUrl();
        }
    }

    /**
     * This is done dynamically because some application servers do not allow
     * the default MBeanServer to be accessed for security reasons (OC4J).
     */
    private void startDefaultServerJMXExport() {
        if (parameterService.is(ParameterConstants.JMX_LEGACY_BEANS_ENABLED)) {
            try {
                getApplicationContext().getBean(Constants.DEFAULT_JMX_SERVER_EXPORTER);
            } catch (Exception ex) {
                log.warn("JMXBeansRegisterError ", ex.getMessage());
            }
        }
    }

    public Properties getProperties() {
        Properties p = new Properties();
        p.putAll(parameterService.getAllParameters());
        return p;
    }

    public String getEngineName() {
        return dbDialect.getEngineName();
    }

    public synchronized void setup() {
        if (!setup) {
            setupDatabase(true);
            setup = true;
        }
    }

    public String reloadNode(String nodeId) {
        return dataService.reloadNode(nodeId);
    }

    public String sendSQL(String nodeId, String tableName, String sql) {
        return dataService.sendSQL(nodeId, tableName, sql);
    }

    public boolean push() {
        return ((IPushService) applicationContext.getBean(Constants.PUSH_SERVICE)).pushData();
    }

    public void syncTriggers() {
        triggerService.syncTriggers();
    }

    public NodeStatus getNodeStatus() {
        return nodeService.getNodeStatus();
    }

    public boolean pull() {
        return ((IPullService) applicationContext.getBean(Constants.PULL_SERVICE)).pullData();
    }

    public void purge() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PURGE_JOB))) {
            purgeService.purge();
        } else {
            throw new UnsupportedOperationException("Cannot actuate a purge if it is already scheduled.");
        }
    }

    protected void setupDatabase(boolean force) {
        configurationService.autoConfigDatabase(force);
        if (upgradeService.isUpgradeNecessary()) {
            if (parameterService.is(ParameterConstants.AUTO_UPGRADE)) {
                try {
                    upgradeService.upgrade();
                    // rerun the auto configuration to make sure things are
                    // kosher after the upgrade
                    configurationService.autoConfigDatabase(force);
                } catch (RuntimeException ex) {
                    log.fatal("SymmetricDSUpgradeFailed", ex);
                    throw ex;
                }
            } else {
                throw new SymmetricException("SymmetricDSUpgradeNeeded");
            }
        }

        // lets do this every time init is called.
        clusterService.initLockTable();
    }

    public void validateConfiguration() {
        Node node = nodeService.findIdentity();
        if (node == null && StringUtils.isBlank(parameterService.getRegistrationUrl())) {
            throw new IllegalStateException(
                    String
                            .format(
                                    "Please set the property %s so this node may pull registration or manually insert configuration into the configuration tables.",
                                    ParameterConstants.REGISTRATION_URL));
        } else if (node != null
                && (!node.getExternalId().equals(parameterService.getExternalId()) || !node.getNodeGroupId().equals(
                        parameterService.getNodeGroupId()))) {
            throw new IllegalStateException(
                    "The configured state does not match recorded database state.  The recorded external id is "
                            + node.getExternalId() + " while the configured external id is "
                            + parameterService.getExternalId() + ".  The recorded node group id is "
                            + node.getNodeGroupId() + " while the configured node group id is "
                            + parameterService.getNodeGroupId());
        } else if (node != null && StringUtils.isBlank(parameterService.getRegistrationUrl()) && StringUtils.isBlank(node.getSyncURL())) {
            throw new IllegalStateException("The sync.url property must be set for the registration server.  Otherwise, registering nodes will not be able to sync with it.");
        }
        // TODO Add more validation checks to make sure that the system is
        // configured correctly

        // TODO Add method to configuration service to validate triggers and
        // call from here.
        // Make sure there are not duplicate trigger rows with the same name
    }

    public void heartbeat(boolean force) {
        dataService.heartbeat(force);
    }

    public void openRegistration(String groupId, String externalId) {
        registrationService.openRegistration(groupId, externalId);
    }

    public void reOpenRegistration(String nodeId) {
        registrationService.reOpenRegistration(nodeId);
    }

    public boolean isRegistered() {
        return nodeService.findIdentity() != null;
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
        return configurationService;
    }

    public IParameterService getParameterService() {
        return parameterService;
    }

    public INodeService getNodeService() {
        return nodeService;
    }

    public IRegistrationService getRegistrationService() {
        return registrationService;
    }

    public IUpgradeService getUpgradeService() {
        return upgradeService;
    }

    public IClusterService getClusterService() {
        return clusterService;
    }

    public IPurgeService getPurgeService() {
        return purgeService;
    }

    public ITriggerRouterService getTriggerService() {
        return triggerService;
    }

    public IDataService getDataService() {
        return dataService;
    }

    public IDbDialect getDbDialect() {
        return dbDialect;
    }

    public IJobManager getJobManager() {
        return jobManager;
    }

}