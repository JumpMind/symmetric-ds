/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric;

import static org.apache.commons.lang.StringUtils.isBlank;

import java.io.File;
import java.io.InputStreamReader;
import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.net.MalformedURLException;
import java.net.URL;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.db.sql.SqlScriptReader;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.security.ISecurityService;
import org.jumpmind.security.SecurityServiceFactory;
import org.jumpmind.security.SecurityServiceFactory.SecurityServiceType;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISoftwareUpgradeListener;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.DefaultOfflineClientListener;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.job.DefaultOfflineServerListener;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfo.ProcessStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IContextService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IExtensionService;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IGroupletService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.ILoadFilterService;
import org.jumpmind.symmetric.service.IMailService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOfflinePullService;
import org.jumpmind.symmetric.service.IOfflinePushService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ISequenceService;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.IUpdateService;
import org.jumpmind.symmetric.service.impl.AcknowledgeService;
import org.jumpmind.symmetric.service.impl.BandwidthService;
import org.jumpmind.symmetric.service.impl.ClusterService;
import org.jumpmind.symmetric.service.impl.ConfigurationService;
import org.jumpmind.symmetric.service.impl.ContextService;
import org.jumpmind.symmetric.service.impl.DataExtractorService;
import org.jumpmind.symmetric.service.impl.DataLoaderService;
import org.jumpmind.symmetric.service.impl.DataService;
import org.jumpmind.symmetric.service.impl.FileSyncExtractorService;
import org.jumpmind.symmetric.service.impl.FileSyncService;
import org.jumpmind.symmetric.service.impl.GroupletService;
import org.jumpmind.symmetric.service.impl.IncomingBatchService;
import org.jumpmind.symmetric.service.impl.LoadFilterService;
import org.jumpmind.symmetric.service.impl.MailService;
import org.jumpmind.symmetric.service.impl.NodeCommunicationService;
import org.jumpmind.symmetric.service.impl.NodeService;
import org.jumpmind.symmetric.service.impl.OfflinePullService;
import org.jumpmind.symmetric.service.impl.OfflinePushService;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.jumpmind.symmetric.service.impl.PullService;
import org.jumpmind.symmetric.service.impl.PurgeService;
import org.jumpmind.symmetric.service.impl.PushService;
import org.jumpmind.symmetric.service.impl.RegistrationService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.service.impl.SequenceService;
import org.jumpmind.symmetric.service.impl.StatisticService;
import org.jumpmind.symmetric.service.impl.TransformService;
import org.jumpmind.symmetric.service.impl.TriggerRouterService;
import org.jumpmind.symmetric.service.impl.UpdateService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportManagerFactory;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;

abstract public class AbstractSymmetricEngine implements ISymmetricEngine {

    private static Map<String, ISymmetricEngine> registeredEnginesByUrl = new HashMap<String, ISymmetricEngine>();
    private static Map<String, ISymmetricEngine> registeredEnginesByName = new HashMap<String, ISymmetricEngine>();

    protected static final Logger log = LoggerFactory.getLogger(AbstractSymmetricEngine.class);

    private boolean started = false;

    private boolean starting = false;

    private boolean setup = false;

    private boolean isInitialized = false;
    
    private Throwable lastException = null;
    
    protected String deploymentType;

    protected ITypedPropertiesFactory propertiesFactory;

    protected IDatabasePlatform platform;

    protected ISecurityService securityService;

    protected ParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;

    protected INodeService nodeService;

    protected IConfigurationService configurationService;

    protected IBandwidthService bandwidthService;

    protected IStatisticService statisticService;

    protected IStatisticManager statisticManager;

    protected IConcurrentConnectionManager concurrentConnectionManager;

    protected ITransportManager transportManager;
    
    protected ITransportManager offlineTransportManager;

    protected IClusterService clusterService;

    protected IPurgeService purgeService;

    protected ITransformService transformService;

    protected ILoadFilterService loadFilterService;

    protected ITriggerRouterService triggerRouterService;

    protected IOutgoingBatchService outgoingBatchService;

    protected IDataService dataService;

    protected IRouterService routerService;

    protected IDataExtractorService dataExtractorService;
    
    protected IDataExtractorService fileSyncExtractorService;

    protected IRegistrationService registrationService;

    protected IDataLoaderService dataLoaderService;

    protected IIncomingBatchService incomingBatchService;

    protected IAcknowledgeService acknowledgeService;

    protected IPushService pushService;

    protected IPullService pullService;

    protected IOfflinePushService offlinePushService;

    protected IOfflinePullService offlinePullService;
    
    protected IJobManager jobManager;

    protected ISequenceService sequenceService;

    protected IExtensionService extensionService;
    
    protected IGroupletService groupletService;

    protected IStagingManager stagingManager;

    protected INodeCommunicationService nodeCommunicationService;
    
    protected IFileSyncService fileSyncService;
    
    protected IMailService mailService;
    
    protected IContextService contextService;
    
    protected IUpdateService updateService;

    protected Date lastRestartTime = null;

    abstract protected ITypedPropertiesFactory createTypedPropertiesFactory();

    abstract protected IDatabasePlatform createDatabasePlatform(TypedProperties properties);

    protected boolean registerEngine = true;

    protected AbstractSymmetricEngine(boolean registerEngine) {
        this.registerEngine = registerEngine;
    }
    
    public static List<ISymmetricEngine> findEngines() {
        List<ISymmetricEngine> engines = new ArrayList<ISymmetricEngine>();
        engines.addAll(registeredEnginesByName.values());
        return engines;
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
            return registeredEnginesByName.get(name);
        } else {
            return null;
        }
    }

    public void setDeploymentType(String deploymentType) {
        this.deploymentType = deploymentType;
    }
    
    protected abstract SecurityServiceType getSecurityServiceType();

    protected void init() {
        if (propertiesFactory == null) {
            this.propertiesFactory = createTypedPropertiesFactory();
        }

        if (securityService == null) {
            this.securityService = SecurityServiceFactory.create(getSecurityServiceType(),
                    propertiesFactory.reload());
        }
        
        TypedProperties properties = this.propertiesFactory.reload();
        
        registerSymDSDriver(properties);
        
        String engineName = properties.get(ParameterConstants.ENGINE_NAME);
        if (!StringUtils.contains(engineName, '`') && !StringUtils.contains(engineName, '(')) {
        	    MDC.put("engineName", engineName);
        }
        this.platform = createDatabasePlatform(properties);

        this.parameterService = new ParameterService(platform, propertiesFactory,
                properties.get(ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX, "sym"));

        boolean parameterTableExists = this.platform.readTableFromDatabase(null, null, 
                TableConstants.getTableName(properties.get(ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX), TableConstants.SYM_PARAMETER)) != null;
        if (parameterTableExists) {
            this.parameterService.setDatabaseHasBeenInitialized(true);
            this.parameterService.rereadParameters();
        }
        
        // So that the key properties are initialized in a predictable order

        parameterService.getNodeGroupId();
        parameterService.getExternalId();
        parameterService.getEngineName();
        parameterService.getSyncUrl();
        parameterService.getRegistrationUrl();
        
        MDC.put("engineName", parameterService.getEngineName());
        
        this.platform.setMetadataIgnoreCase(this.parameterService
                .is(ParameterConstants.DB_METADATA_IGNORE_CASE));
        this.platform.setClearCacheModelTimeoutInMs(parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TABLES_IN_MS));


        this.symmetricDialect = createSymmetricDialect();
        this.extensionService = createExtensionService();
        this.extensionService.refresh();
        this.symmetricDialect.setExtensionService(extensionService);
        this.parameterService.setExtensionService(extensionService);

        this.bandwidthService = new BandwidthService(parameterService);
        this.sequenceService = new SequenceService(parameterService, symmetricDialect);
        this.stagingManager = createStagingManager();
        this.nodeService = new NodeService(this);
        this.configurationService = new ConfigurationService(parameterService, symmetricDialect,
                nodeService);
        this.dataService = new DataService(this, extensionService);
        this.clusterService = createClusterService();
        this.statisticService = new StatisticService(parameterService, symmetricDialect);
        this.statisticManager = createStatisticManager();
        this.concurrentConnectionManager = new ConcurrentConnectionManager(parameterService,
                statisticManager);
        this.purgeService = new PurgeService(parameterService, symmetricDialect, clusterService,
                statisticManager, extensionService);
        this.transformService = new TransformService(parameterService, symmetricDialect,
                configurationService, extensionService);
        this.loadFilterService = new LoadFilterService(parameterService, symmetricDialect,
                configurationService);
        this.groupletService = new GroupletService(this);
        this.triggerRouterService = new TriggerRouterService(this);
        this.outgoingBatchService = new OutgoingBatchService(parameterService, symmetricDialect,
                nodeService, configurationService, sequenceService, clusterService, extensionService);
        this.routerService = buildRouterService();
        this.nodeCommunicationService = buildNodeCommunicationService(clusterService, nodeService, parameterService, configurationService, symmetricDialect);
        this.incomingBatchService = new IncomingBatchService(parameterService, symmetricDialect, clusterService);
        this.dataExtractorService = new DataExtractorService(this);
        this.transportManager = new TransportManagerFactory(this).create();
        this.offlineTransportManager = new TransportManagerFactory(this).create(Constants.PROTOCOL_FILE);
        this.dataLoaderService = new DataLoaderService(this);
        this.registrationService = new RegistrationService(this);
        this.acknowledgeService = new AcknowledgeService(this);
        this.pushService = new PushService(parameterService, symmetricDialect,
                dataExtractorService, acknowledgeService, transportManager, nodeService,
                clusterService, nodeCommunicationService, statisticManager, configurationService, extensionService);
        this.pullService = new PullService(parameterService, symmetricDialect, 
                nodeService, dataLoaderService, registrationService, clusterService, nodeCommunicationService, 
                configurationService, extensionService, statisticManager);
        this.offlinePushService = new OfflinePushService(parameterService, symmetricDialect,
                dataExtractorService, acknowledgeService, offlineTransportManager, nodeService,
                clusterService, nodeCommunicationService, statisticManager, configurationService, extensionService);
        this.offlinePullService = new OfflinePullService(parameterService, symmetricDialect, 
                nodeService, dataLoaderService, clusterService, nodeCommunicationService, 
                configurationService, extensionService, offlineTransportManager);
        this.fileSyncService = buildFileSyncService();
        this.fileSyncExtractorService = new FileSyncExtractorService(this);
        this.mailService = new MailService(parameterService, symmetricDialect);
        this.contextService = new ContextService(parameterService, symmetricDialect);

        String updateServiceClassName = properties.get(ParameterConstants.UPDATE_SERVICE_CLASS);
        if (updateServiceClassName == null) {
            this.updateService = new UpdateService(this);
        } else {
            try {
                Constructor<?> cons = Class.forName(updateServiceClassName).getConstructor(ISymmetricEngine.class);
                this.updateService = (IUpdateService) cons.newInstance(this);
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
        
        this.jobManager = createJobManager();

        extensionService.addExtensionPoint(new DefaultOfflineServerListener(
                statisticManager, nodeService, outgoingBatchService));

        IOfflineClientListener defaultlistener = new DefaultOfflineClientListener(parameterService,
                nodeService);
        extensionService.addExtensionPoint(defaultlistener);

        if (registerEngine) {
            registerHandleToEngine();
        }

    }

    protected void registerSymDSDriver(TypedProperties engineProperties) {
        try {            
            Class<?> driverClass = Thread.currentThread().getContextClassLoader().loadClass("org.jumpmind.driver.Driver");
            if (driverClass != null) {
                Method method = driverClass.getMethod("register", TypedProperties.class);
                method.invoke(null, engineProperties);
            }
        } catch (Exception ex) {
            log.debug("Failed to load org.jumpmind.driver.Driver", ex);
        }
    }

    protected IClusterService createClusterService() {
        return new ClusterService(parameterService, symmetricDialect, nodeService);
    }

    protected IRouterService buildRouterService() {
        return new RouterService(this);
    }
    
    protected IFileSyncService buildFileSyncService() {
        return new FileSyncService(this);
    }    

    protected INodeCommunicationService buildNodeCommunicationService(IClusterService clusterService, INodeService nodeService, IParameterService parameterService, 
    		IConfigurationService configurationService, ISymmetricDialect symmetricDialect) {
        return new NodeCommunicationService(clusterService, nodeService, parameterService, configurationService, symmetricDialect);
    }

    abstract protected IStagingManager createStagingManager();

    abstract protected IStatisticManager createStatisticManager();

    abstract protected ISymmetricDialect createSymmetricDialect();

    abstract protected IExtensionService createExtensionService();

    abstract protected IJobManager createJobManager();

    public String getSyncUrl() {
        return parameterService.getSyncUrl();
    }

    public Properties getProperties() {
        Properties p = new Properties();
        p.putAll(parameterService.getAllParameters());
        return p;
    }

    public String getEngineName() {
    	return parameterService.getEngineName();
    }

    public void setup() {
        if (!setup) {
            setupDatabase(false);
            parameterService.setDatabaseHasBeenInitialized(true);
            
            String databaseVersion = this.getNodeService().findIdentity() != null ? 
            		this.getNodeService().findIdentity().getSymmetricVersion() : null;
    		String softwareVersion = Version.version();
    		
    		log.info("SymmetricDS database version : " + databaseVersion);
    		log.info("SymmetricDS software version : " + softwareVersion);
    		
    		if (databaseVersion != null && !softwareVersion.equals(databaseVersion)) {
    			log.info("SymmetricDS database version does not match the current software version, running software upgrade listeners.");
	            List<ISoftwareUpgradeListener> softwareUpgradeListeners = 
	            		extensionService.getExtensionPointList(ISoftwareUpgradeListener.class);
	            for (ISoftwareUpgradeListener listener : softwareUpgradeListeners) {
	            	listener.upgrade(databaseVersion, softwareVersion);
	            }
    		}
            
            setup = true;
        }
    }

    public void setupDatabase(boolean force) {
        log.info("Initializing SymmetricDS database");
        if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE) || force) {
            symmetricDialect.initTablesAndDatabaseObjects();
        } else {
            log.info("SymmetricDS is not configured to auto-create the database");
        }
        try {
            configurationService.initDefaultChannels();
        } catch (SqlException e) {
            if (e.getCause() instanceof SQLException) {
                SQLException se = (SQLException) e.getCause();
                if (se.getErrorCode() == -7008 && se.getSQLState().equals("55019")) {
                    log.error("Please enable journaling on SYM objects.  For instructions, see the appendix in the User Guide on DB2 for i.");
                }
            }
            throw e;
        }
        clusterService.init();
        sequenceService.init();
        autoConfigRegistrationServer();
        log.info("Done initializing SymmetricDS database");
    }

    protected void autoConfigRegistrationServer() {
        Node node = nodeService.findIdentity();

        if (node == null) {
            buildTablesFromDdlUtilXmlIfProvided();
            loadFromScriptIfProvided();
            parameterService.rereadParameters();
        }

        node = nodeService.findIdentity();

        if (node == null && parameterService.isRegistrationServer()
                && parameterService.is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false)) {
            log.info("Inserting rows for node, security, identity and group for registration server");
            String nodeId = parameterService.getExternalId();
            node = new Node(parameterService, symmetricDialect);
            node.setNodeId(node.getExternalId());
            nodeService.save(node);
            nodeService.insertNodeIdentity(nodeId);
            node = nodeService.findIdentity();
            nodeService.insertNodeGroup(node.getNodeGroupId(), null);
            NodeSecurity nodeSecurity = nodeService.findOrCreateNodeSecurity(nodeId);
            nodeSecurity.setInitialLoadTime(new Date());
            nodeSecurity.setRegistrationTime(new Date());
            nodeSecurity.setInitialLoadEnabled(false);
            nodeSecurity.setRegistrationEnabled(false);
            nodeService.updateNodeSecurity(nodeSecurity);
        }
    }

    protected boolean buildTablesFromDdlUtilXmlIfProvided() {
        boolean loaded = false;
        String xml = parameterService
                .getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML);
        if (!StringUtils.isBlank(xml)) {
            File file = new File(xml);
            URL fileUrl = null;
            if (file.isFile()) {
                try {
                    fileUrl = file.toURI().toURL();
                } catch (MalformedURLException e) {
                    throw new RuntimeException(e);
                }
            } else {
                fileUrl = getClass().getResource(xml);
            }

            if (fileUrl != null) {
                try {
                    log.info("Building database schema from: {}", xml);
                    Database database = DatabaseXmlUtil.read(new InputStreamReader(fileUrl
                            .openStream()));
                    IDatabasePlatform platform = symmetricDialect.getPlatform();
                    platform.createDatabase(database, true, true);
                    loaded = true;
                } catch (Exception e) {
                    log.error("", e);
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
    protected boolean loadFromScriptIfProvided() {
        boolean loaded = false;
        String sqlScripts = parameterService
                .getString(ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT);
        if (!StringUtils.isBlank(sqlScripts)) {
            String[] sqlScriptList = sqlScripts.split(",");
            for (String sqlScript : sqlScriptList) {
                sqlScript = sqlScript.trim();
                if (StringUtils.isNotBlank(sqlScript)) {
                    File file = new File(sqlScript);
                    URL fileUrl = null;
                    if (file.isFile()) {
                        try {
                            fileUrl = file.toURI().toURL();
                        } catch (MalformedURLException e) {
                            throw new RuntimeException(e);
                        }
                    } else {
                        fileUrl = getClass().getResource(sqlScript);
                        if (fileUrl == null) {
                            fileUrl = Thread.currentThread().getContextClassLoader()
                                    .getResource(sqlScript);
                        }
                    }

                    if (fileUrl != null) {
                        log.info("Executing {} '{}' ({})", ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT, sqlScript, fileUrl);
                        new SqlScript(fileUrl, symmetricDialect.getPlatform().getSqlTemplate(),
                                true, SqlScriptReader.QUERY_ENDS, getSymmetricDialect().getPlatform()
                                        .getSqlScriptReplacementTokens()).execute();
                        loaded = true;
                    } else {
                        log.warn("Could not find the {}: '{}' to execute.  We would have run it if we had found it", ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT, sqlScript);
                    }
                }
            }
        }
        return loaded;
    }

    public synchronized boolean start() {
        return start(true);
    }

    public synchronized boolean start(boolean startJobs) {
        isInitialized = false;
        if (!starting && !started) {
            try {
                starting = true;
                symmetricDialect.verifyDatabaseIsCompatible();
                setup();
                if (isConfigured()) {
                    Node node = nodeService.findIdentity();
                    if (node != null && (!node.getExternalId().equals(getParameterService().getExternalId())
                            || !node.getNodeGroupId().equals(
                                    getParameterService().getNodeGroupId()))) {
                        if (parameterService.is(ParameterConstants.NODE_COPY_MODE_ENABLED,
                                false)) {
                            registrationService.requestNodeCopy();
                        } else {
                            throw new SymmetricException(
                                    "The configured state does not match recorded database state.  The recorded external id is '%s' while the configured external id is '%s'. The recorded node group id is '%s' while the configured node group id is '%s'",
                                    new Object[] { node.getExternalId(),
                                            getParameterService().getExternalId(),
                                            node.getNodeGroupId(),
                                            getParameterService().getNodeGroupId() });
                        }
                        isInitialized = true;
                   } else if (node != null) {
                        
                        log.info(
                                "Starting registered node [group={}, id={}, nodeId={}]",
                                new Object[] { node.getNodeGroupId(), node.getNodeId(),
                                        node.getExternalId() });

                        isInitialized = true;
                        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS_AT_STARTUP,
                                true)) {
                            triggerRouterService.syncTriggers();
                        } else {
                            log.info(ParameterConstants.AUTO_SYNC_TRIGGERS_AT_STARTUP
                                    + " is turned off");
                        }

                        if (parameterService
                                .is(ParameterConstants.HEARTBEAT_SYNC_ON_STARTUP, false)
                                || isBlank(node.getDatabaseType())
                                || !StringUtils.equals(node.getSyncUrl(),
                                        parameterService.getSyncUrl())) {
                            heartbeat(false);
                        }

                        if (parameterService.is(ParameterConstants.AUTO_SYNC_CONFIG_AT_STARTUP, true)) {
                            pullService.pullConfigData(false);
                        }

                    } else {
                        log.info("Starting unregistered node [group={}, externalId={}]",
                                parameterService.getNodeGroupId(), parameterService.getExternalId());
                        isInitialized = true;
                    }

                    if (jobManager != null) {
                        jobManager.init();
                    }
                    
                    if (startJobs && jobManager != null) {
                        jobManager.startJobs();
                    }
                    
                    if (parameterService.isRegistrationServer()) {
                        this.updateService.init();
                    }
                    
                    lastRestartTime = new Date();
                    statisticManager.incrementRestart();
                    started = true;

                } else {
                    log.error("Did not start SymmetricDS.  It has not been configured properly");
                }
            } catch (Throwable ex) {                
                log.error("An error occurred while starting SymmetricDS", ex);
                lastException = ex;
                /* Don't leave SymmetricDS in a half started state */
                stop();
            } finally {
                starting = false;
            }
        }

        if (started) {
            log.info(getEngineDescription("STARTED:"));
        } else {
            log.info(getEngineDescription("NOT STARTED:"));
        }

        return started;
    }
    
    public String getEngineDescription(String msg) {
        if (lastRestartTime == null) {
            return "";
        }
        String formattedUptime = FormatUtils.formatDurationReadable(System.currentTimeMillis() - lastRestartTime.getTime());
        
        return String.format( "SymmetricDS Node %s\n\t nodeId=%s\n\t groupId=%s\n\t type=%s\n\t name=%s\n\t softwareVersion=%s\n\t databaseName=%s\n\t databaseVersion=%s\n\t driverName=%s\n\t driverVersion=%s\n\t uptime=%s",
                 msg, getParameterService().getExternalId(), getParameterService().getNodeGroupId(), 
                getDeploymentType(), getEngineName(), Version.version(), symmetricDialect.getName(),
                symmetricDialect.getVersion(), symmetricDialect.getDriverName(),
                symmetricDialect.getDriverVersion(), formattedUptime );        
    }
    
    
    
    public synchronized void uninstall() {
        
        log.info("Attempting an uninstall of all SymmetricDS database objects from the database");
        
        stop();
        
        log.info("Just cleaned {} files in the staging area during the uninstall.", getStagingManager().clean(0));
        
        try {
            String prefix = parameterService.getTablePrefix();
            
            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_GROUPLET)) != null) {
                groupletService.deleteAllGrouplets();                
            }

            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_TRIGGER_ROUTER)) != null) {
                triggerRouterService.deleteAllTriggerRouters();
            }

            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_FILE_TRIGGER_ROUTER)) != null) {
                fileSyncService.deleteAllFileTriggerRouters();                
            }

            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_ROUTER)) != null) {
                triggerRouterService.deleteAllRouters();
            }

            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_CONFLICT)) != null) {
                dataLoaderService.deleteAllConflicts();
            }

            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_TRANSFORM_TABLE)) != null) {
                transformService.deleteAllTransformTables();
            }
            
            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_ROUTER)) != null) {
                triggerRouterService.deleteAllRouters();
            }
            
            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_CONFLICT)) != null) {
                dataLoaderService.deleteAllConflicts();
            }
            
            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_NODE_GROUP_LINK)) != null) {
                configurationService.deleteAllNodeGroupLinks();
            }

            if (platform.readTableFromDatabase(null, null, TableConstants.getTableName(prefix, TableConstants.SYM_LOCK)) != null) {
               // this should remove all triggers because we have removed all the trigger configuration
               triggerRouterService.syncTriggers();            
            }
        } catch (SqlException ex) {
            log.warn("Error while trying to remove triggers on tables", ex);
        }
        
        // remove any additional triggers that may remain because they were not in trigger history
        symmetricDialect.cleanupTriggers();                
        
        log.info("Removing SymmetricDS database objects");
        symmetricDialect.dropTablesAndDatabaseObjects();
        
        // force cache to be cleared
        nodeService.deleteIdentity();
        
        parameterService.setDatabaseHasBeenInitialized(false);
        
        log.info("Finished uninstalling SymmetricDS database objects from the database");
        
    }    

    public synchronized void stop() {
        
        log.info("Stopping SymmetricDS externalId={} version={} database={}",
                new Object[] { parameterService == null ? "?" : parameterService.getExternalId(), Version.version(),
                        symmetricDialect == null? "?":symmetricDialect.getName() });
        if (jobManager != null) {
            jobManager.stopJobs();
        }
        if (routerService != null) {
        	routerService.stop();
        }
        if (nodeCommunicationService != null) {
        	nodeCommunicationService.stop();
        }
        if (updateService != null) {
            updateService.stop();
        }
        
        if (statisticManager != null) {
            List<ProcessInfo> infos = statisticManager.getProcessInfos();
            for (ProcessInfo processInfo : infos) {
                Thread thread = processInfo.getThread();
                if (processInfo.getStatus() != ProcessStatus.OK && thread.isAlive()) {
                    log.info("Trying to interrupt thread '{}' ", thread.getName());
                    try {
                        thread.interrupt();
                    } catch (Exception e) {
                        log.info("Caught exception while attempting to interrupt thread", e);
                    }
                }
            }

            Thread.interrupted();
        }
        
        started = false;
        starting = false;
        
    }

    public synchronized void destroy() {
        removeMeFromMap(registeredEnginesByName);        
        removeMeFromMap(registeredEnginesByUrl);
        if (parameterService != null) {
            parameterService.setDatabaseHasBeenInitialized(false);
            registeredEnginesByName.remove(getEngineName());
            registeredEnginesByUrl.remove(getSyncUrl());
        }
        stop();
        if (jobManager != null) {
            jobManager.destroy();
        }
    }

    public String reloadNode(String nodeId, String createBy) {
        return dataService.reloadNode(nodeId, false, createBy);
    }

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName,
            String sql) {
        return dataService.sendSQL(nodeId, catalogName, schemaName, tableName, sql);
    }

    public RemoteNodeStatuses push() {
        MDC.put("engineName", getEngineName());
        return pushService.pushData(true);
    }

    public void syncTriggers() {
        MDC.put("engineName", getEngineName());
        triggerRouterService.syncTriggers();
    }

    public void forceTriggerRebuild() {
        MDC.put("engineName", getEngineName());
        triggerRouterService.syncTriggers(true);
    }

    public NodeStatus getNodeStatus() {
        return nodeService.getNodeStatus();
    }
    
    public void removeAndCleanupNode(String nodeId) {
        log.info("Removing node {}", nodeId);
        nodeService.deleteNode(nodeId, false);
        log.info("Done removing node ID {}", nodeId);        
    }

    public RemoteNodeStatuses pull() {
        MDC.put("engineName", getEngineName());
        return pullService.pullData(true);
    }

    public void route() {
        MDC.put("engineName", getEngineName());
        routerService.routeData(true);
    }

    public void purge() {
        MDC.put("engineName", getEngineName());
        purgeService.purgeOutgoing(true);
        purgeService.purgeIncoming(true);
    }    

    public boolean isConfigured() {
        boolean configurationValid = false;

        boolean isRegistrationServer = getNodeService().isRegistrationServer();

        boolean isSelfConfigurable = isRegistrationServer
                && (getParameterService().is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND,
                        false) || StringUtils.isNotBlank(getParameterService().getString(
                        ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT)));

        Table symNodeTable = symmetricDialect.getPlatform().readTableFromDatabase(null, null,
                TableConstants.getTableName(parameterService.getTablePrefix(),
                        TableConstants.SYM_NODE));

        Node node = symNodeTable != null ? getNodeService().findIdentity() : null;

        long offlineNodeDetectionPeriodSeconds = getParameterService().getLong(
                ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES) * 60;
        long heartbeatSeconds = getParameterService().getLong(
                ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);

        String registrationUrl = getParameterService().getRegistrationUrl();

        if (!isSelfConfigurable && node == null && isRegistrationServer) {
            log.warn(
                    "This node is configured as a registration server, but it is missing its node_identity.  It probably needs configured.",
                    ParameterConstants.REGISTRATION_URL);
        } else if (!isSelfConfigurable && node == null
                && StringUtils.isBlank(getParameterService().getRegistrationUrl())) {
            log.warn(
                    "Please set the property {} so this node may pull registration or manually insert configuration into the configuration tables",
                    ParameterConstants.REGISTRATION_URL);
        } else if (Constants.PLEASE_SET_ME.equals(registrationUrl)) {
            log.warn("Please set the registration.url for the node");
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getNodeGroupId())) {
            log.warn("Please set the group.id for the node");
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getExternalId())) {
            log.warn("Please set the external.id for the node");            
        } else if (offlineNodeDetectionPeriodSeconds > 0
                && offlineNodeDetectionPeriodSeconds <= heartbeatSeconds) {
            // Offline node detection is not disabled (-1) and the value is too
            // small (less than the heartbeat)
            log.warn(
                    "The {} property must be a longer period of time than the {} property.  Otherwise, nodes will be taken offline before the heartbeat job has a chance to run",
                    ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES,
                    ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);
        } else if (node != null && Version.isOlderMinorVersion(Version.version(), node.getSymmetricVersion())) {
            log.warn("SymmetricDS does not support automatic downgrading.  The current version running version of {} is older than the last running version of {}", 
                    Version.version(), node.getSymmetricVersion());
        } else {
            if (node != null && Version.isOlderMinorVersion(node.getSymmetricVersion(), Version.version())) {
                log.debug("The current version of {} is newer than the last running version of {}", 
                        Version.version(), node.getSymmetricVersion());
            }
            configurationValid = true;
        }

        return configurationValid;
    }

    public void heartbeat(boolean force) {
        MDC.put("engineName", getEngineName());
        dataService.heartbeat(force);
    }

    public void openRegistration(String nodeGroupId, String externalId) {
        MDC.put("engineName", getEngineName());
        registrationService.openRegistration(nodeGroupId, externalId);
    }
    
    public void clearCaches() {
        getTriggerRouterService().clearCache();
        getParameterService().rereadParameters();
        getTransformService().clearCache();
        getDataLoaderService().clearCache();
        getConfigurationService().initDefaultChannels();
        getConfigurationService().clearCache();
        getNodeService().flushNodeAuthorizedCache();
        getJobManager().startJobsAfterConfigChange();
    }

    public void reOpenRegistration(String nodeId) {
        MDC.put("engineName", getEngineName());
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

    public boolean isInitialized() {
        return isInitialized;
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

    public IClusterService getClusterService() {
        return clusterService;
    }

    public IPurgeService getPurgeService() {
        return purgeService;
    }

    public IDataService getDataService() {
        return dataService;
    }

    public IJobManager getJobManager() {
        return this.jobManager;
    }

    public IOutgoingBatchService getOutgoingBatchService() {
        return outgoingBatchService;
    }

    public IAcknowledgeService getAcknowledgeService() {
        return this.acknowledgeService;
    }

    public IBandwidthService getBandwidthService() {
        return bandwidthService;
    }

    public IDataExtractorService getDataExtractorService() {
        return this.dataExtractorService;
    }
    
    public IDataExtractorService getFileSyncExtractorService() {
        return this.fileSyncExtractorService;
    }
   
    public IDataLoaderService getDataLoaderService() {
        return this.dataLoaderService;
    }

    public IIncomingBatchService getIncomingBatchService() {
        return this.incomingBatchService;
    }

    public IPullService getPullService() {
        return this.pullService;
    }

    public IPushService getPushService() {
        return this.pushService;
    }

    public IOfflinePullService getOfflinePullService() {
        return this.offlinePullService;
    }

    public IOfflinePushService getOfflinePushService() {
        return this.offlinePushService;
    }

    public IRouterService getRouterService() {
        return this.routerService;
    }

    public ISecurityService getSecurityService() {
        return securityService;
    }

    public IStatisticService getStatisticService() {
        return statisticService;
    }

    public IStatisticManager getStatisticManager() {
        return statisticManager;
    }

    public ITriggerRouterService getTriggerRouterService() {
        return triggerRouterService;
    }

    public String getDeploymentType() {
        return deploymentType;
    }

    public ITransformService getTransformService() {
        return this.transformService;
    }

    public ILoadFilterService getLoadFilterService() {
        return this.loadFilterService;
    }

    public IConcurrentConnectionManager getConcurrentConnectionManager() {
        return concurrentConnectionManager;
    }

    public String getTablePrefix() {
        return parameterService.getTablePrefix();
    }

    public ITransportManager getTransportManager() {
        return transportManager;
    }

    public ITransportManager getOfflineTransportManager() {
        return offlineTransportManager;
    }

    public IExtensionService getExtensionService() {
        return extensionService;
    }
    
    public IMailService getMailService() {
        return mailService;
    }

    public IContextService getContextService() {
        return contextService;
    }

    public IStagingManager getStagingManager() {
        return stagingManager;
    }

    public ISequenceService getSequenceService() {
        return sequenceService;
    }

    public INodeCommunicationService getNodeCommunicationService() {
        return nodeCommunicationService;
    }
    
    public IGroupletService getGroupletService() {
        return groupletService;
    }
    
    public String getLastException() {
        return lastException.getMessage();
    }
    
    private void removeMeFromMap(Map<String, ISymmetricEngine> map) {
        Set<String> keys = new HashSet<String>(map.keySet());
        for (String key : keys) {
            if (this.equals(map.get(key))) {
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
    private void registerHandleToEngine() {
        String url = getSyncUrl();
        ISymmetricEngine alreadyRegister = registeredEnginesByUrl.get(url);
        if (alreadyRegister == null || alreadyRegister.equals(this)) {
            if (url != null) {
                registeredEnginesByUrl.put(url, this);
            }
        } else {
            log.warn("Could not register engine.  There was already an engine registered under the url: {}",
                            getSyncUrl());
        }

        alreadyRegister = registeredEnginesByName.get(getEngineName());
        if (alreadyRegister == null || alreadyRegister.equals(this)) {
            registeredEnginesByName.put(getEngineName(), this);
        } else {
            throw new EngineAlreadyRegisteredException(
                    "Could not register engine.  There was already an engine registered under the name: "
                            + getEngineName());
        }

    }

    public Date getLastRestartTime() {
        return lastRestartTime;
    }

    public ISqlTemplate getSqlTemplate() {
        return getSymmetricDialect().getPlatform().getSqlTemplate();
    }

    public Logger getLog() {
        return log;
    }

    @SuppressWarnings("unchecked")
    public <T> T getDataSource() {
        return (T) getSymmetricDialect().getPlatform().getDataSource();
    }
    
    public IDatabasePlatform getDatabasePlatform() {
        return getSymmetricDialect().getPlatform();
    }
    
    public IFileSyncService getFileSyncService() {
        return fileSyncService;
    }
    
    public IUpdateService getUpdateService() {
    	return updateService;
    }
    
    @Override
    public String getNodeId() {
        return getNodeService().findIdentityNodeId();
    }
    
    @Override 
    public String toString() {
        return "Engine " + getNodeId() + " " + super.toString();
    }

}
