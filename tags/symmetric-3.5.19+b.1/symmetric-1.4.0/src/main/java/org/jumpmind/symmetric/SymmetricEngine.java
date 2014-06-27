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

import java.sql.SQLException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;

import javax.sql.DataSource;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.job.PullJob;
import org.jumpmind.symmetric.job.PurgeJob;
import org.jumpmind.symmetric.job.PushJob;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This is the preferred way to create, configure, start and manage an instance
 * of Symmetric. The engine will bootstrap the symmetric.xml Spring context.
 * <p/> The Symmetric instance is configured by properties configuration files.
 * By default the engine will look for and override existing properties with
 * ones found in the properties files. Symmetric looks for: symmetric.properties
 * in the classpath (it will use the first one it finds), and then for a
 * symmetric.properties found in the user.home system property location. Next,
 * if provided, in the constructor of the SymmetricEngine, it will locate and
 * use the properties file passed to the engine. <p/> When the engine is ready
 * to be started, the {@link #start()} method should be called. It should only
 * be called once.
 */
public class SymmetricEngine {

    protected static final Log logger = LogFactory.getLog(SymmetricEngine.class);

    private ApplicationContext applicationContext;

    private IBootstrapService bootstrapService;

    private IParameterService parameterService;

    private INodeService nodeService;

    private IRegistrationService registrationService;

    private IPurgeService purgeService;

    private IDataService dataService;

    private boolean started = false;

    private boolean starting = false;

    private boolean setup = false;

    private IDbDialect dbDialect;

    private Set<Timer> jobs;

    private static Map<String, SymmetricEngine> registeredEnginesByUrl = new HashMap<String, SymmetricEngine>();

    private static Map<String, SymmetricEngine> registeredEnginesByName = new HashMap<String, SymmetricEngine>();

    public SymmetricEngine(String... overridePropertiesResources) {
        String one = null;
        String two = null;
        if (overridePropertiesResources.length > 0) {
            one = overridePropertiesResources[0];
            if (overridePropertiesResources.length > 1) {
                two = overridePropertiesResources[1];
            }
        }
        init(one, two);
    }

    /**
     * Create a symmetric node
     */
    public SymmetricEngine() {
        init(createContext());
    }

    /**
     * Pass in the Spring context to be used. This had better include the Spring
     * configuration for required Symmetric services.
     * 
     * @param ctx
     *                A Spring framework context
     */
    protected SymmetricEngine(ApplicationContext ctx) {
        init(ctx);
    }

    public void stop() {
        logger.info("Closing SymmetricDS externalId=" + parameterService.getExternalId() + " version="
                + Version.version() + " database=" + dbDialect.getName());
        stopJobs();
        removeMeFromMap(registeredEnginesByName);
        removeMeFromMap(registeredEnginesByUrl);
        DataSource ds = dbDialect.getJdbcTemplate().getDataSource();
        if (ds instanceof BasicDataSource) {
            try {
                ((BasicDataSource) ds).close();
            } catch (SQLException ex) {
                logger.error(ex, ex);
            }
        }
        applicationContext = null;
        bootstrapService = null;
        parameterService = null;
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

    private ApplicationContext createContext() {
        return new ClassPathXmlApplicationContext("classpath:/symmetric.xml");
    }

    /**
     * @param overridePropertiesResource1
     *                Provide a Spring resource path to a properties file to be
     *                used for configuration
     * @param overridePropertiesResource2
     *                Provide a Spring resource path to a properties file to be
     *                used for configuration
     */
    private void init(String overridePropertiesResource1, String overridePropertiesResource2) {
        // Setting system properties is probably not the best way to accomplish
        // this setup.
        // Synchronizing on the class so creating multiple engines is thread
        // safe.
        synchronized (SymmetricEngine.class) {
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_1, overridePropertiesResource1 == null ? ""
                    : overridePropertiesResource1);
            System.setProperty(Constants.OVERRIDE_PROPERTIES_FILE_2, overridePropertiesResource2 == null ? ""
                    : overridePropertiesResource2);
            this.init(createContext());
        }
    }

    private void init(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        bootstrapService = (IBootstrapService) applicationContext.getBean(Constants.BOOTSTRAP_SERVICE);
        parameterService = (IParameterService) applicationContext.getBean(Constants.PARAMETER_SERVICE);
        nodeService = (INodeService) applicationContext.getBean(Constants.NODE_SERVICE);
        registrationService = (IRegistrationService) applicationContext.getBean(Constants.REGISTRATION_SERVICE);
        purgeService = (IPurgeService) applicationContext.getBean(Constants.PURGE_SERVICE);
        dataService = (IDataService) applicationContext.getBean(Constants.DATA_SERVICE);
        dbDialect = (IDbDialect) applicationContext.getBean(Constants.DB_DIALECT);
    }

    /**
     * Register this instance of the engine so it can be found by other
     * processes in the JVM.
     * 
     * @see #findEngineByUrl(String)
     */
    private void registerEngine() {
        registeredEnginesByUrl.put(parameterService.getMyUrl(), this);
        registeredEnginesByName.put(getEngineName(), this);
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
                logger.warn("Unable to register JMX beans with the default MBeanServer. " + ex.getMessage());
            }
        }
    }

    private void startJob(String name) {
        if (jobs == null) {
            jobs = new HashSet<Timer>();
        }
        logger.info("Starting " + name);
        Timer timer = AppUtils.find(name, this);
        jobs.add(timer);
    }

    /**
     * Start the jobs if they are configured to be started in
     * symmetric.properties
     */
    private void startJobs() {
        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PUSH_JOB))) {
            startJob(Constants.PUSH_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PULL_JOB))) {
            startJob(Constants.PULL_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PURGE_JOB))) {
            startJob(Constants.PURGE_JOB_TIMER);
        }

        if (Boolean.TRUE.toString()
                .equalsIgnoreCase(parameterService.getString(ParameterConstants.START_HEARTBEAT_JOB))) {
            startJob(Constants.HEARTBEAT_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_SYNCTRIGGERS_JOB))) {
            startJob(Constants.SYNC_TRIGGERS_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                parameterService.getString(ParameterConstants.START_STATISTIC_FLUSH_JOB))) {
            startJob(Constants.STATISTIC_FLUSH_JOB_TIMER);
        }

    }

    private void stopJobs() {
        if (jobs != null) {
            for (Timer job : jobs) {
                try {
                    job.cancel();
                } catch (RuntimeException e) {
                    logger.error(e, e);
                }
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

    public String getEngineName() {
        return dbDialect.getEngineName();
    }

    /**
     * Will setup the symmetric tables, if not already setup and if the engine
     * is configured to do so.
     */
    public synchronized void setup() {
        if (!setup) {
            bootstrapService.setupDatabase();
            setup = true;
        }
    }

    /**
     * Must be called to start symmetric.
     */
    public synchronized void start() {
        if (!starting && !started) {
            starting = true;
            setup();
            registerEngine();
            startDefaultServerJMXExport();
            Node node = nodeService.findIdentity();
            if (node != null) {
                logger.info("Starting registered node [group=" + node.getNodeGroupId() + ", id=" + node.getNodeId()
                        + ", externalId=" + node.getExternalId() + "]");
            } else {
                logger.info("Starting unregistered node [group=" + parameterService.getNodeGroupId() + ", externalId="
                        + parameterService.getExternalId() + "]");
            }
            bootstrapService.validateConfiguration();
            bootstrapService.syncTriggers();
            startJobs();
            started = true;
            logger.info("Started SymmetricDS externalId=" + parameterService.getExternalId() + " version="
                    + Version.version() + " database=" + dbDialect.getName());
            starting = false;

        }
    }

    /**
     * Queue up an initial load or a reload to a node.
     */
    public String reloadNode(String nodeId) {
        return dataService.reloadNode(nodeId);
    }

    /**
     * This can be called if the push job has not been enabled. It will perform
     * a push the same way the {@link PushJob} would have.
     * 
     * @see IPushService#pushData()
     */
    public void push() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PUSH_JOB))) {
            ((IPushService) applicationContext.getBean(Constants.PUSH_SERVICE)).pushData();
        } else {
            throw new UnsupportedOperationException("Cannot actuate a push if it is already scheduled.");
        }
    }

    /**
     * Call this to resync triggers
     * 
     * @see IBootstrapService#syncTriggers()
     */
    public void syncTriggers() {
        bootstrapService.syncTriggers();
    }

    /**
     * This can be called if the pull job has not been enabled. It will perform
     * a pull the same way the {@link PullJob} would have.
     * 
     * @see IPullService#pullData()
     */
    public void pull() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(parameterService.getString(ParameterConstants.START_PULL_JOB))) {
            ((IPullService) applicationContext.getBean(Constants.PULL_SERVICE)).pullData();
        } else {
            throw new UnsupportedOperationException("Cannot actuate a push if it is already scheduled.");
        }
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

    /**
     * Push a copy of the node onto the push queue so the symmetric node
     * 'checks' in with it's root node.
     * 
     * @see IBootstrapService#heartbeat()
     */
    public void heartbeat() {
        bootstrapService.heartbeat();
    }

    /**
     * Open up registration for node to attach.
     * 
     * @see IRegistrationService#openRegistration(String, String)
     */
    public void openRegistration(String groupId, String externalId) {
        registrationService.openRegistration(groupId, externalId);
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
     * @return
     */
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }

    /**
     * Locate a {@link SymmetricEngine} in the same JVM
     */
    public static SymmetricEngine findEngineByUrl(String url) {
        return registeredEnginesByUrl.get(url);
    }

    public static SymmetricEngine findEngineByName(String name) {
        return registeredEnginesByName.get(name);
    }

}
