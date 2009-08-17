/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric;

import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.Platform;
import org.apache.ddlutils.io.DatabaseIO;
import org.apache.ddlutils.model.Database;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.job.PullJob;
import org.jumpmind.symmetric.job.PurgeJob;
import org.jumpmind.symmetric.job.PushJob;
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

/**
 * This is the preferred way to create, configure, start and manage a
 * client-only instance of SymmetricDS. The engine will bootstrap the
 * symmetric.xml Spring context.
 * <p/>
 * The SymmetricDS instance is configured by properties configuration files. By
 * default the engine will look for and override existing properties with ones
 * found in the properties files. SymmetricDS looks for: symmetric.properties in
 * the classpath (it will use the first one it finds), and then for a
 * symmetric.properties found in the user.home system property location. Next,
 * if provided, in the constructor of the SymmetricEngine, it will locate and
 * use the properties file passed to the engine.
 * <p/>
 * When the engine is ready to be started, the {@link #start()} method should be
 * called. It should only be called once.
 */
public class SymmetricEngine {

    protected static final ILog log = LogFactory.getLog(SymmetricEngine.class);

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

    private static Map<String, SymmetricEngine> registeredEnginesByUrl = new HashMap<String, SymmetricEngine>();

    private static Map<String, SymmetricEngine> registeredEnginesByName = new HashMap<String, SymmetricEngine>();

    public SymmetricEngine(String... overridePropertiesResources) {
        this(null, overridePropertiesResources);
    }

    /**
     * Create a SymmetricDS instance using an existing
     * {@link ApplicationContext} as the parent. This gives the SymmetricDS
     * context access to beans in the parent context.
     * 
     * @param parentContext
     * @param overridePropertiesResources
     */
    public SymmetricEngine(ApplicationContext parentContext, String... overridePropertiesResources) {
        String one = null;
        String two = null;
        if (overridePropertiesResources.length > 0 && overridePropertiesResources[0] != null) {
            one = overridePropertiesResources[0].trim();
        }
        if (overridePropertiesResources.length > 1 && overridePropertiesResources[1] != null) {
            two = overridePropertiesResources[1].trim();
        }
        init(parentContext, one, two);
    }

    /**
     * Create a SymmetricDS node. This constructor creates a new
     * {@link ApplicationContext} using SymmetricDS's classpath:/symmetric.xml.
     */
    public SymmetricEngine() {
        init(createContext(null));
    }

    /**
     * Pass in the {@link ApplicationContext} to be used. The context passed in
     * needs to load classpath:/symmetric.xml.
     * 
     * @param ctx
     *            A Spring framework context to use for this SymmetricEngine
     */
    public SymmetricEngine(ApplicationContext ctx) {
        init(ctx);
    }

    public void stop() {
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

    private void removeMeFromMap(Map<String, SymmetricEngine> map) {
        Set<String> keys = new HashSet<String>(map.keySet());
        for (String key : keys) {
            if (map.get(key).equals(this)) {
                map.remove(key);
            }
        }
    }

    private ApplicationContext createContext(ApplicationContext parentContext) {
        return new ClassPathXmlApplicationContext(new String[] { "classpath:/symmetric.xml" }, parentContext);
    }

    /**
     * @param overridePropertiesResource1
     *            Provide a Spring resource path to a properties file to be used
     *            for configuration
     * @param overridePropertiesResource2
     *            Provide a Spring resource path to a properties file to be used
     *            for configuration
     */
    private void init(ApplicationContext parentContext, String overridePropertiesResource1,
            String overridePropertiesResource2) {
        // Setting system properties is probably not the best way to accomplish
        // this setup.
        // Synchronizing on the class so creating multiple engines is thread
        // safe.
        synchronized (SymmetricEngine.class) {
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, overridePropertiesResource1 == null ? ""
                    : overridePropertiesResource1);
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_2, overridePropertiesResource2 == null ? ""
                    : overridePropertiesResource2);
            this.init(createContext(parentContext));
        }
    }

    private void init(ApplicationContext applicationContext) {
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

    /**
     * @return the URL that represents this engine
     */
    public String getMyUrl() {
        Node node = nodeService.findIdentity();
        if (node != null) {
            return node.getSyncURL();
        } else {
            return parameterService.getMyUrl();
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

    /**
     * Get a list of configured properties for Symmetric. Read-only.
     */
    public Properties getProperties() {
        Properties p = new Properties();
        p.putAll(parameterService.getAllParameters());
        return p;
    }

    /**
     * @return The lower case representation of the engine name as setup in the
     *         symmetric.properties file. We always use a lower case
     *         representation because there are times the engine name is used in
     *         triggers at which point you can lose the original case
     *         representation.
     */
    public String getEngineName() {
        return dbDialect.getEngineName();
    }

    /**
     * Will setup the SymmetricDS tables, if not already setup and if the engine
     * is configured to do so.
     */
    public synchronized void setup() {
        if (!setup) {
            setupDatabase(true);
            setup = true;
        }
    }

    /**
     * Must be called to start SymmetricDS.
     */
    public synchronized void start() {
        if (!starting && !started) {
            try {
                starting = true;
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
                heartbeat();
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

    /**
     * Queue up an initial load or a reload to a node.
     */
    public String reloadNode(String nodeId) {
        return dataService.reloadNode(nodeId);
    }

    public String sendSQL(String nodeId, String tableName, String sql) {
        return dataService.sendSQL(nodeId, tableName, sql);
    }

    /**
     * Will perform a push the same way the {@link PushJob} would have.
     * 
     * @see IPushService#pushData()
     * @return true if data was pushed successfully
     */
    public boolean push() {
        return ((IPushService) applicationContext.getBean(Constants.PUSH_SERVICE)).pushData();
    }

    /**
     * Call this to resync triggers
     * 
     * @see ITriggerRouterService#syncTriggers()
     */
    public void syncTriggers() {
        triggerService.syncTriggers();
    }

    /**
     * Get the current status of this node.
     * 
     * @return {@link NodeStatus}
     */
    public NodeStatus getNodeStatus() {
        return nodeService.getNodeStatus();
    }

    /**
     * Will perform a pull the same way the {@link PullJob} would have.
     * 
     * @see IPullService#pullData()
     */
    public boolean pull() {
        return ((IPullService) applicationContext.getBean(Constants.PULL_SERVICE)).pullData();
    }

    /**
     * This can be called to do a purge. It may be called only if the
     * {@link PurgeJob} has not been enabled.
     * 
     * @see IPurgeService#purge()
     */
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

        if (nodeService.findIdentity() == null) {
            buildTablesFromDdlUtilXmlIfProvided();
            loadFromScriptIfProvided();
        }

        // lets do this every time init is called.
        clusterService.initLockTable();
    }

    /**
     * Simply check and make sure that this node is all configured properly for
     * operation.
     */
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
        }
        // TODO Add more validation checks to make sure that the system is
        // configured correctly

        // TODO Add method to configuration service to validate triggers and
        // call from here.
        // Make sure there are not duplicate trigger rows with the same name
    }

    private boolean buildTablesFromDdlUtilXmlIfProvided() {
        boolean loaded = false;
        String xml = parameterService.getString(ParameterConstants.AUTO_CONFIGURE_REGISTRATION_SERVER_DDLUTIL_XML);
        if (!StringUtils.isBlank(xml)) {
            File file = new File(xml);
            URL fileUrl = null;
            if (file.isFile()) {
                try {
                    fileUrl = file.toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            } else {
                fileUrl = getClass().getResource(xml);
            }

            if (fileUrl != null) {
                try {
                    log.info("DatabaseSchemaBuilding", xml);
                    Database database = new DatabaseIO().read(new InputStreamReader(fileUrl.openStream()));
                    Platform platform = dbDialect.getPlatform();
                    platform.createTables(database, false, true);
                    loaded = true;
                } catch (Exception e) {
                    log.error(e);
                }
            }
        }
        return loaded;
    }

    /**
     * Give the end user the option to provide a script that will load a
     * registration server with an initial SymmetricDS setup.
     * 
     * Look first on the file system, then in the classpath for the SQL file.
     * 
     * @return true if the script was executed
     */
    private boolean loadFromScriptIfProvided() {
        boolean loaded = false;
        String sqlScript = parameterService.getString(ParameterConstants.AUTO_CONFIGURE_REGISTRATION_SERVER_SQL_SCRIPT);
        if (!StringUtils.isBlank(sqlScript)) {
            File file = new File(sqlScript);
            URL fileUrl = null;
            if (file.isFile()) {
                try {
                    fileUrl = file.toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            } else {
                fileUrl = getClass().getResource(sqlScript);
            }

            if (fileUrl != null) {
                log.info("ScriptRunning", sqlScript);
                new SqlScript(fileUrl, dbDialect.getJdbcTemplate().getDataSource(), true).execute();
                loaded = true;
            }
        }
        return loaded;
    }

    /**
     * Push a copy of the node onto the push queue so the SymmetricDS node
     * 'checks' in with it's root node.
     * 
     * @see IconfigurationService#heartbeat()
     */
    public void heartbeat() {
        dataService.heartbeat();
    }

    /**
     * Open up registration for node to attach.
     * 
     * @see IRegistrationService#openRegistration(String, String)
     */
    public void openRegistration(String groupId, String externalId) {
        registrationService.openRegistration(groupId, externalId);
    }

    public void reOpenRegistration(String nodeId) {
        registrationService.reOpenRegistration(nodeId);
    }

    /**
     * Check to see if this node has been registered.
     * 
     * @return true if the node is registered
     */
    public boolean isRegistered() {
        return nodeService.findIdentity() != null;
    }

    /**
     * Check to see if this node has been started.
     * 
     * @return true if the node is started
     */
    public boolean isStarted() {
        return started;
    }

    /**
     * Check to see if this node is starting.
     * 
     * @return true if the node is starting
     */

    public boolean isStarting() {
        return starting;
    }

    /**
     * Expose access to the Spring context. This is for advanced use only.
     * 
     * @return the Spring application context that SymmetricDS runs in
     */
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /**
     * Locate a {@link SymmetricEngine} in the same JVM
     */
    public static SymmetricEngine findEngineByUrl(String url) {
        if (registeredEnginesByUrl != null && url != null) {
            return registeredEnginesByUrl.get(url);
        } else {
            return null;
        }
    }

    /**
     * Locate a {@link SymmetricEngine} in the same JVM
     */
    public static SymmetricEngine findEngineByName(String name) {
        if (registeredEnginesByName != null && name != null) {
            return registeredEnginesByName.get(name.toLowerCase());
        } else {
            return null;
        }
    }

/**
     * Locate the one and only registered {@link SymmetricEngine}.  Use {@link #findEngineByName(String)} or
     * {@link #findEngineByUrl(String) if there is more than on engine registered.
     * @throws IllegalStateException This exception happens if more than one engine is 
     * registered  
     */
    public static SymmetricEngine getEngine() {
        int numberOfEngines = registeredEnginesByName.size();
        if (numberOfEngines == 0) {
            return null;
        } else if (numberOfEngines > 1) {
            throw new IllegalStateException("More than one SymmetricEngine is currently registered");
        } else {
            return registeredEnginesByName.values().iterator().next();
        }
    }

}
