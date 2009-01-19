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

package org.jumpmind.mule.transport.symmetric;

import java.util.List;
import java.util.Properties;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.PropertiesConstants;
import org.jumpmind.symmetric.config.IRuntimeConfig;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.job.PullJob;
import org.jumpmind.symmetric.job.PurgeJob;
import org.jumpmind.symmetric.job.PushJob;
import org.jumpmind.symmetric.model.BatchInfo;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtractListener;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
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
public class SimpleSymmetricEngine {

    protected static final Log logger = LogFactory
            .getLog(SimpleSymmetricEngine.class);

    private ApplicationContext applicationContext;

    private IBootstrapService bootstrapService;

    private IRuntimeConfig runtimeConfig;

    private INodeService nodeService;

    private IPurgeService purgeService;
    
    private IDataService dataService;

    private IDataExtractorService dataExtractorService;

    private IAcknowledgeService acknowledgeService;

    private boolean started = false;
    
    private boolean starting = false;
    
    private IDbDialect dbDialect;

    private Properties properties;

    /**
     * Create a symmetric node
     */
    public SimpleSymmetricEngine() {
        init(createContext());
    }
    
    private ApplicationContext createContext() {
        return new ClassPathXmlApplicationContext("classpath:/simple-symmetric.xml");
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
        purgeService = (IPurgeService) applicationContext
                .getBean(Constants.PURGE_SERVICE);               
        dataService = (IDataService)applicationContext.getBean(Constants.DATA_SERVICE);
        dataExtractorService = (IDataExtractorService)applicationContext.getBean(Constants.DATAEXTRACTOR_SERVICE);
        acknowledgeService = (IAcknowledgeService)applicationContext.getBean(Constants.ACKNOWLEDGE_SERVICE);
         dbDialect = (IDbDialect) applicationContext.getBean(Constants.DB_DIALECT);
        logger.info("Initialized SymmetricDS externalId=" + runtimeConfig.getExternalId() + " version=" + Version.version() + " database="+dbDialect.getName());
    }
    
    /**
     * Get a list of configured properties for Symmetric.  Read-only.
     */
    public Properties getProperties() {
        return new Properties(properties);
    }
    
    public String getEngineName() {
        return dbDialect.getEngineName();
    }

    /**
     * Must be called to start symmetric.
     */
    public synchronized void start() {
        if (!starting) {
        	starting = true;
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
            started = true;
        }
    }
    
    /**
     * Queue up an initial load or a reload to a node.
     */
    public void reloadNode(String nodeId) {
        dataService.reloadNode(nodeId);
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
     * Check to see if this node has been registered.
     * @return true if the node is registered
     */
    public boolean isRegistered() {
        return nodeService.findIdentity() != null;
    }

    /**
     * Check to see if this node has been started.
     * @return true if the node is started
     */
	public boolean isStarted() {
		return started;
	}
	
    /**
     * Check to see if this node is starting.
     * @return true if the node is starting
     */

	public boolean isStarting() {
		return starting;
	}
	
    /**
     * Expose access to the Spring context.  This is for advanced use only.
     * @return
     */
    public ApplicationContext getApplicationContext() {
        return applicationContext;
    }
    
    public boolean extract(IExtractListener extractListener) 
    {
        try
        {
            return dataExtractorService.extract(nodeService.findIdentity(), extractListener);
        }
        // why is extract throwing an Exception?
        catch (Exception e)
        {
            throw new IllegalStateException(e);
        }
    }

    public void acknowledge(List<BatchInfo> batches) 
    {
        acknowledgeService.ack(batches);
    }
}
