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
 * under the License. 
 */
package org.jumpmind.symmetric;

import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
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
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IExtensionPointManager;
import org.jumpmind.symmetric.io.DefaultOfflineClientListener;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.job.DefaultOfflineServerListener;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IIncomingBatchService;
import org.jumpmind.symmetric.service.ILoadFilterService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPullService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.IPushService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.service.ISecurityService;
import org.jumpmind.symmetric.service.ISequenceService;
import org.jumpmind.symmetric.service.IStatisticService;
import org.jumpmind.symmetric.service.ITransformService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.impl.AcknowledgeService;
import org.jumpmind.symmetric.service.impl.BandwidthService;
import org.jumpmind.symmetric.service.impl.ClusterService;
import org.jumpmind.symmetric.service.impl.ConfigurationService;
import org.jumpmind.symmetric.service.impl.DataExtractorService;
import org.jumpmind.symmetric.service.impl.DataLoaderService;
import org.jumpmind.symmetric.service.impl.DataService;
import org.jumpmind.symmetric.service.impl.IncomingBatchService;
import org.jumpmind.symmetric.service.impl.LoadFilterService;
import org.jumpmind.symmetric.service.impl.NodeCommunicationService;
import org.jumpmind.symmetric.service.impl.NodeService;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.jumpmind.symmetric.service.impl.PullService;
import org.jumpmind.symmetric.service.impl.PurgeService;
import org.jumpmind.symmetric.service.impl.PushService;
import org.jumpmind.symmetric.service.impl.RegistrationService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.service.impl.SecurityService;
import org.jumpmind.symmetric.service.impl.SequenceService;
import org.jumpmind.symmetric.service.impl.StatisticService;
import org.jumpmind.symmetric.service.impl.TransformService;
import org.jumpmind.symmetric.service.impl.TriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportManagerFactory;
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

    protected IClusterService clusterService;

    protected IPurgeService purgeService;

    protected ITransformService transformService;

    protected ILoadFilterService loadFilterService;

    protected ITriggerRouterService triggerRouterService;

    protected IOutgoingBatchService outgoingBatchService;

    protected IDataService dataService;

    protected IRouterService routerService;

    protected IDataExtractorService dataExtractorService;

    protected IRegistrationService registrationService;

    protected IDataLoaderService dataLoaderService;

    protected IIncomingBatchService incomingBatchService;

    protected IAcknowledgeService acknowledgeService;

    protected IPushService pushService;

    protected IPullService pullService;

    protected IJobManager jobManager;

    protected ISequenceService sequenceService;

    protected IExtensionPointManager extensionPointManger;

    protected IStagingManager stagingManager;

    protected INodeCommunicationService nodeCommunicationService;

    protected Date lastRestartTime = null;

    abstract protected ITypedPropertiesFactory createTypedPropertiesFactory();

    abstract protected IDatabasePlatform createDatabasePlatform(TypedProperties properties);

    protected boolean registerEngine = true;

    protected AbstractSymmetricEngine(boolean registerEngine) {
        this.registerEngine = registerEngine;
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

    protected void init() {
        this.propertiesFactory = createTypedPropertiesFactory();
        this.securityService = createSecurityService(propertiesFactory.reload());
        TypedProperties properties = this.propertiesFactory.reload();
        this.platform = createDatabasePlatform(properties);
        this.parameterService = new ParameterService(platform, propertiesFactory, properties.get(
                ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX, "sym"));

        MDC.put("engineName", this.parameterService.getEngineName());

        this.platform.setMetadataIgnoreCase(this.parameterService
                .is(ParameterConstants.DB_METADATA_IGNORE_CASE));
        this.platform.setClearCacheModelTimeoutInMs(parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TABLES_IN_MS));

        this.bandwidthService = new BandwidthService(parameterService);
        this.symmetricDialect = createSymmetricDialect();
        this.sequenceService = new SequenceService(parameterService, symmetricDialect);
        this.stagingManager = createStagingManager();
        this.nodeService = new NodeService(parameterService, symmetricDialect);
        this.configurationService = new ConfigurationService(parameterService, symmetricDialect,
                nodeService);
        this.clusterService = new ClusterService(parameterService, symmetricDialect);
        this.statisticService = new StatisticService(parameterService, symmetricDialect);
        this.statisticManager = new StatisticManager(parameterService, nodeService,
                configurationService, statisticService, clusterService);
        this.concurrentConnectionManager = new ConcurrentConnectionManager(parameterService,
                statisticManager);
        this.purgeService = new PurgeService(parameterService, symmetricDialect, clusterService,
                statisticManager);
        this.transformService = new TransformService(parameterService, symmetricDialect,
                configurationService);
        this.loadFilterService = new LoadFilterService(parameterService, symmetricDialect,
                configurationService);
        this.triggerRouterService = new TriggerRouterService(parameterService, symmetricDialect,
                clusterService, configurationService, statisticManager);
        this.outgoingBatchService = new OutgoingBatchService(parameterService, symmetricDialect,
                nodeService, configurationService, sequenceService, clusterService);
        this.dataService = new DataService(this);
        this.routerService = buildRouterService();
        this.dataExtractorService = new DataExtractorService(parameterService, symmetricDialect,
                outgoingBatchService, routerService, configurationService, triggerRouterService,
                nodeService, dataService, transformService, statisticManager, stagingManager);
        this.incomingBatchService = new IncomingBatchService(parameterService, symmetricDialect, clusterService);
        this.transportManager = new TransportManagerFactory(this).create();
        this.dataLoaderService = new DataLoaderService(this);
        this.registrationService = new RegistrationService(parameterService, symmetricDialect,
                nodeService, dataExtractorService, dataService, dataLoaderService,
                transportManager, statisticManager);
        this.acknowledgeService = new AcknowledgeService(parameterService, symmetricDialect,
                outgoingBatchService, registrationService, stagingManager);
        this.nodeCommunicationService = buildNodeCommunicationService(clusterService, nodeService, parameterService, symmetricDialect);
        this.pushService = new PushService(parameterService, symmetricDialect,
                dataExtractorService, acknowledgeService, transportManager, nodeService,
                clusterService, nodeCommunicationService);
        this.pullService = new PullService(parameterService, symmetricDialect, nodeService,
                dataLoaderService, registrationService, clusterService, nodeCommunicationService);
        this.jobManager = createJobManager();

        this.nodeService.addOfflineServerListener(new DefaultOfflineServerListener(
                statisticManager, nodeService, outgoingBatchService));

        IOfflineClientListener defaultlistener = new DefaultOfflineClientListener(parameterService,
                nodeService);
        this.pullService.addOfflineListener(defaultlistener);
        this.pushService.addOfflineListener(defaultlistener);

        if (registerEngine) {
            registerHandleToEngine();
        }

    }

    protected IRouterService buildRouterService() {
        return new RouterService(this);
    }

    protected INodeCommunicationService buildNodeCommunicationService(IClusterService clusterService, INodeService nodeService, IParameterService parameterService, ISymmetricDialect symmetricDialect) {
        return new NodeCommunicationService(clusterService, nodeService, parameterService, symmetricDialect);
    }

    public static ISecurityService createSecurityService(TypedProperties properties) {
        try {
            String className = properties.get(ParameterConstants.CLASS_NAME_SECURITY_SERVICE,
                    SecurityService.class.getName());
            ISecurityService securityService = (ISecurityService) Class.forName(className).newInstance();
            securityService.init();
            return securityService;
        } catch (RuntimeException e) {
            throw e;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    abstract protected IStagingManager createStagingManager();

    abstract protected ISymmetricDialect createSymmetricDialect();

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
        return parameterService.getString(ParameterConstants.ENGINE_NAME);
    }

    public void setup() {
        getParameterService().rereadParameters();
        if (!setup) {
            setupDatabase(false);
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
        configurationService.initDefaultChannels();
        clusterService.init();
        sequenceService.init();
        autoConfigRegistrationServer();
        parameterService.rereadParameters();
        log.info("Done initializing SymmetricDS database");
    }

    protected void autoConfigRegistrationServer() {
        Node node = nodeService.findIdentity();

        if (node == null) {
            buildTablesFromDdlUtilXmlIfProvided();
            loadFromScriptIfProvided();
        }

        node = nodeService.findIdentity();

        if (node == null && StringUtils.isBlank(parameterService.getRegistrationUrl())
                && parameterService.is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND, false)) {
            log.info("Inserting rows for node, security, identity and group for registration server");
            String nodeId = parameterService.getExternalId();
            node = new Node(parameterService, symmetricDialect);
            node.setNodeId(node.getExternalId());
            nodeService.save(node);
            nodeService.insertNodeIdentity(nodeId);
            node = nodeService.findIdentity();
            nodeService.insertNodeGroup(node.getNodeGroupId(), null);
            NodeSecurity nodeSecurity = nodeService.findNodeSecurity(nodeId, true);
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
                    log.error(e.getMessage(), e);
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
                        new SqlScript(fileUrl, symmetricDialect.getPlatform().getSqlTemplate(),
                                true, SqlScriptReader.QUERY_ENDS, getSymmetricDialect().getPlatform()
                                        .getSqlScriptReplacementTokens()).execute();
                        loaded = true;
                    } else {
                        log.info("Could not find the sql script: {} to execute.  We would have run it if we had found it");
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
        setup();
        if (isConfigured()) {
            if (!starting && !started) {
                try {
                    starting = true;
                    Node node = nodeService.findIdentity();
                    if (node != null) {
                        log.info(
                                "Starting registered node [group={}, id={}, externalId={}]",
                                new Object[] { node.getNodeGroupId(), node.getNodeId(),
                                        node.getExternalId() });
                    } else {
                        log.info("Starting unregistered node [group={}, externalId={}]",
                                parameterService.getNodeGroupId(), parameterService.getExternalId());
                    }
                    triggerRouterService.syncTriggers();
                    heartbeat(false);
                    if (startJobs && jobManager != null) {
                        jobManager.startJobs();
                    }
                    log.info("Started SymmetricDS");
                    lastRestartTime = new Date();
                    started = true;
                } finally {
                    starting = false;
                }

            }
        } else {
            log.warn("Did not start SymmetricDS.  It has not been configured properly");
        }

        log.info(
                "SymmetricDS: type={}, name={}, version={}, groupId={}, externalId={}, databaseName={}, databaseVersion={}, driverName={}, driverVersion={}",
                new Object[] { getDeploymentType(), getEngineName(), Version.version(),
                        getParameterService().getNodeGroupId(),
                        getParameterService().getExternalId(), symmetricDialect.getName(),
                        symmetricDialect.getVersion(), symmetricDialect.getDriverName(),
                        symmetricDialect.getDriverVersion() });
        return started;
    }
    
    
    public synchronized void uninstall() {
        
        log.warn("Attempting an uninstall of all SymmetricDS database objects from the database");
        
        stop();
        
        try {
            
            List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRouters();
            for (TriggerRouter triggerRouter : triggerRouters) {
                triggerRouterService.deleteTriggerRouter(triggerRouter);
            }

            for (TriggerRouter triggerRouter : triggerRouters) {
                triggerRouterService.deleteTrigger(triggerRouter.getTrigger());
                triggerRouterService.deleteRouter(triggerRouter.getRouter());
            }
            
            // remove the links so the symmetric table trigger will be removed
            List<NodeGroupLink> links = configurationService.getNodeGroupLinks();
            for (NodeGroupLink nodeGroupLink : links) {
                configurationService.deleteNodeGroupLink(nodeGroupLink);
            }

            // this should remove all triggers because we have removed all the
            // trigger configuration
            triggerRouterService.syncTriggers();

        } catch (SqlException ex) {
            // these tables must not exist
        }

        // remove any additional triggers that may remain because they were not in trigger history
        symmetricDialect.cleanupTriggers();                
        
        symmetricDialect.dropTablesAndDatabaseObjects();
        
        // force cache to be cleared
        nodeService.deleteIdentity();
        
        parameterService.setDatabaseHasBeenInitialized(false);
        
        log.warn("Finished uninstalling SymmetricDS database objects from the database");
        
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
        
        started = false;
        starting = false;
    }

    public synchronized void destroy() {
        stop();
        if (jobManager != null) {
            jobManager.destroy();
        }
        removeMeFromMap(registeredEnginesByName);
        removeMeFromMap(registeredEnginesByUrl);
    }

    public String reloadNode(String nodeId) {
        return dataService.reloadNode(nodeId, false);
    }

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName,
            String sql) {
        return dataService.sendSQL(nodeId, catalogName, schemaName, tableName, sql, false);
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
        purgeService.purgeDataGaps(true);
    }

    public boolean isConfigured() {
        boolean configurationValid = false;

        boolean isRegistrationServer = getNodeService().isRegistrationServer();

        boolean isSelfConfigurable = isRegistrationServer
                && (getParameterService().is(ParameterConstants.AUTO_INSERT_REG_SVR_IF_NOT_FOUND,
                        false) || StringUtils.isNotBlank(getParameterService().getString(
                        ParameterConstants.AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT)));

        Table symNodeTable = symmetricDialect.getPlatform().getTableFromCache(
                TableConstants.getTableName(parameterService.getTablePrefix(),
                        TableConstants.SYM_NODE), true);

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
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getExternalId())
                || Constants.PLEASE_SET_ME.equals(registrationUrl)
                || Constants.PLEASE_SET_ME.equals(getParameterService().getNodeGroupId())) {
            log.warn("Please set the registration.url, node.group.id, and external.id for the node");
        } else if (node != null
                && (!node.getExternalId().equals(getParameterService().getExternalId()) || !node
                        .getNodeGroupId().equals(getParameterService().getNodeGroupId()))) {
            log.warn(
                    "The configured state does not match recorded database state.  The recorded external id is {} while the configured external id is {}. The recorded node group id is {} while the configured node group id is {}",
                    new Object[] { node.getExternalId(), getParameterService().getExternalId(),
                            node.getNodeGroupId(), getParameterService().getNodeGroupId() });

        } else if (node != null && StringUtils.isBlank(getParameterService().getRegistrationUrl())
                && StringUtils.isBlank(getParameterService().getSyncUrl())) {
            log.warn("The sync.url property must be set for the registration server.  Otherwise, registering nodes will not be able to sync with it");

        } else if (offlineNodeDetectionPeriodSeconds > 0
                && offlineNodeDetectionPeriodSeconds <= heartbeatSeconds) {
            // Offline node detection is not disabled (-1) and the value is too
            // small (less than the heartbeat)
            log.warn(
                    "The {} property must be a longer period of time than the {} property.  Otherwise, nodes will be taken offline before the heartbeat job has a chance to run",
                    ParameterConstants.OFFLINE_NODE_DETECTION_PERIOD_MINUTES,
                    ParameterConstants.HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC);
        } else if (node != null && Version.isOlderThanVersion(Version.version(), node.getSymmetricVersion())) {
            log.warn("SymmetricDS does not support automatic downgrading.  The current version running version of {} is older than the last running version of {}", 
                    Version.version(), node.getSymmetricVersion());
        } else {
            if (node != null && Version.isOlderThanVersion(node.getSymmetricVersion(), Version.version())) {
                log.info("The current version of {} is newer than the last running version of {}", 
                        Version.version(), node.getSymmetricVersion());
            }
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
        MDC.put("engineName", getEngineName());
        dataService.heartbeat(true);
    }

    public void openRegistration(String nodeGroupId, String externalId) {
        MDC.put("engineName", getEngineName());
        registrationService.openRegistration(nodeGroupId, externalId);
    }
    
    public void clearCaches() {
        getTriggerRouterService().resetCaches();
        getParameterService().rereadParameters();
        getTransformService().resetCache();
        getDataLoaderService().reloadConflictNodeGroupLinks();
        getConfigurationService().reloadChannels();
        getNodeService().flushNodeAuthorizedCache();
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

    public ISymmetricDialect getSymmetricDialect() {
        return symmetricDialect;
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

    public IExtensionPointManager getExtensionPointManager() {
        return extensionPointManger;
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

}
