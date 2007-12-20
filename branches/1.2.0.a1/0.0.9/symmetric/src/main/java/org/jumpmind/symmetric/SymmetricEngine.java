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

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.PropertiesConstants;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.job.PurgeJob;
import org.jumpmind.symmetric.job.PushJob;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

/**
 * This is the preferred way to create, configure, start and manage an instance of
 * Symmetric.  The engine will bootstrap the symmetric.xml Spring context.
 * <p/>
 * The Symmetric instance is configured by properties configuration files.  By default the engine 
 * will look for and override existing properties with ones found in the properties files.  Symmetric looks 
 * for:  symmetric.properties in the classpath (it will use the first one it finds), and then for a symmetric.properties found 
 * in the user.home system property location.  Next, if provided, in the constructor of the SymmetricEngine, it will
 * locate and use the properties file passed to the engine.
 * <p/>
 * When the engine is ready to be started, the {@link #start()} method should be called.  It should only be called once.
 */
public class SymmetricEngine {

    protected static final Log logger = LogFactory
            .getLog(SymmetricEngine.class);

    private ApplicationContext applicationContext;

    private IBootstrapService bootstrapService;

    private IRuntimeConfig runtimeConfig;

    private INodeService nodeService;

    private IRegistrationService registrationService;

    private IPurgeService purgeService;

    private boolean started = false;

    private Properties properties;

    private static Map<String, SymmetricEngine> registeredEnginesByUrl = new HashMap<String, SymmetricEngine>();

    /**
     * @param overridePropertiesResource1 Provide a Spring resource path to a properties file to be used for configuration
     * @param overridePropertiesResource2 Provide a Spring resource path to a properties file to be used for configuration
     */
    public SymmetricEngine(String overridePropertiesResource1,
            String overridePropertiesResource2) {
        // Setting system properties is probably not the best way to accomplish this setup.
        // Synchronizing on the class so creating multiple engines is thread safe.
        synchronized (SymmetricEngine.class) {
            System.setProperty("symmetric.override.properties.file.1",
                    overridePropertiesResource1);
            System.setProperty("symmetric.override.properties.file.2",
                    overridePropertiesResource2);
            this.init(createContext());
        }
    }

    /**
     * Create a symmetric node
     */
    public SymmetricEngine() {
        init(createContext());
    }

    /**
     * Pass in the Spring context to be used.  This had better include the Spring configuration for required Symmetric services.
     * @param ctx A Spring framework context
     */
    protected SymmetricEngine(ApplicationContext ctx) {
        init(ctx);
    }

    private ApplicationContext createContext() {
        return new ClassPathXmlApplicationContext("classpath:/symmetric.xml");
    }

    private void init(ApplicationContext applicationContext) {
        this.applicationContext = applicationContext;
        properties = (Properties) applicationContext
                .getBean(Constants.PROPERTIES);
        bootstrapService = (IBootstrapService) applicationContext
                .getBean(Constants.BOOTSTRAP_SERVICE);
        runtimeConfig = (IRuntimeConfig) applicationContext
                .getBean(Constants.RUNTIME_CONFIG);
        nodeService = (INodeService) applicationContext
                .getBean(Constants.NODE_SERVICE);
        registrationService = (IRegistrationService) applicationContext
                .getBean(Constants.REGISTRATION_SERVICE);
        purgeService = (IPurgeService) applicationContext
                .getBean(Constants.PURGE_SERVICE);
        registerEngine();
        logger.info("Initialized SymmetricDS version " + Version.VERSION);
    }

    /**
     * Register this instance of the engine so it can be found by other processes in the JVM.
     * @see #findEngineByUrl(String)
     */
    private void registerEngine() {
        registeredEnginesByUrl.put(runtimeConfig.getMyUrl(), this);
    }

    /**
     * Start the jobs if they are configured to be started in symmetric.properties
     */
    private void startJobs() {
        if (Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PUSH_JOB))) {
            applicationContext.getBean(Constants.PUSH_JOB_TIMER);
        }
        if (Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PULL_JOB))) {
            applicationContext.getBean(Constants.PULL_JOB_TIMER);
        }

        if (Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PURGE_JOB))) {
            applicationContext.getBean(Constants.PURGE_JOB_TIMER);
        }

        if (Boolean.TRUE
                .toString()
                .equalsIgnoreCase(
                        properties
                                .getProperty(PropertiesConstants.START_HEARTBEAT_JOB))) {
            applicationContext.getBean(Constants.HEARTBEAT_JOB_TIMER);
        }

        if (Boolean.TRUE
                .toString()
                .equalsIgnoreCase(
                        properties
                                .getProperty(PropertiesConstants.START_SYNCTRIGGERS_JOB))) {
            applicationContext.getBean(Constants.SYNC_TRIGGERS_JOB_TIMER);
        }
    }

    /**
     * Must be called to start symmetric.
     */
    public synchronized void start() {
        if (!started) {
        	bootstrapService.init();
            Node node = nodeService.findIdentity();
            if (node != null) {
                logger.info("Starting registered node [group=" + node.getNodeGroupId() +
                        ", id=" + node.getNodeId() + ", externalId=" + node.getExternalId() + "]");
            } else {
                logger.info("Starting unregistered node [group=" + runtimeConfig.getNodeGroupId() +
                        ", externalId=" + runtimeConfig.getExternalId() + "]");
            }
            bootstrapService.register();
            bootstrapService.syncTriggers();
            startJobs();
            started = true;
        }
    }

    /**
     * This can be called if the push job has not been enabled.  It will perform a push
     * the same way the {@link PushJob} would have.
     * @see IPushService#pushData()
     */
    public void push() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PUSH_JOB))) {
            ((IPushService) applicationContext.getBean(Constants.PUSH_SERVICE))
                    .pushData();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot actuate a push if it is already scheduled.");
        }
    }

    /**
     * Call this to resync triggers
     * @see IBootstrapService#syncTriggers()
     */
    public void syncTriggers() {
        bootstrapService.syncTriggers();
    }

    /**
     * This can be called if the pull job has not been enabled.  It will perform a pull
     * the same way the {@link PullJob} would have.
     * @see IPullService#pullData()
     */
    public void pull() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PULL_JOB))) {
            ((IPullService) applicationContext.getBean(Constants.PULL_SERVICE))
                    .pullData();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot actuate a push if it is already scheduled.");
        }
    }

    /**
     * This can be called to do a purge.  It may be called only if the {@link PurgeJob} has not been enabled.
     * @see IPurgeService#purge()
     */
    public void purge() {
        if (!Boolean.TRUE.toString().equalsIgnoreCase(
                properties.getProperty(PropertiesConstants.START_PURGE_JOB))) {
            purgeService.purge();
        } else {
            throw new UnsupportedOperationException(
                    "Cannot actuate a purge if it is already scheduled.");
        }
    }

    /**
     * Push a copy of the node onto the push queue so the symmetric node 'checks' in with it's root node.
     * @see IBootstrapService#heartbeat()
     */
    public void heartbeat() {
        bootstrapService.heartbeat();
    }

    /**
     * Open up registration for client to attach.  
     * @see IRegistrationService#openRegistration(String, String)
     */
    public void openRegistration(String groupId, String externalId) {
        registrationService.openRegistration(groupId, externalId);
    }

    /**
     * Check to see if this node has been registered.
     * @return true if the node is registered
     */
    public boolean isRegistered() {
        return nodeService.findIdentity() != null;
    }

    /**
     * Expose access to the Spring context.  This is for advanced use only.
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

}
