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

import java.io.File;
import java.io.InputStreamReader;
import java.net.MalformedURLException;
import java.net.URL;
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
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IExtensionPointManager;
import org.jumpmind.symmetric.io.DefaultOfflineClientListener;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.job.DefaultOfflineServerListener;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.Grouplet;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.NodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.TableReloadRequest;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IAcknowledgeService;
import org.jumpmind.symmetric.service.IBandwidthService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IFileSyncService;
import org.jumpmind.symmetric.service.IGroupletService;
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
import org.jumpmind.symmetric.service.impl.DataLoaderService.ConflictNodeGroupLink;
import org.jumpmind.symmetric.service.impl.DataService;
import org.jumpmind.symmetric.service.impl.FileSyncService;
import org.jumpmind.symmetric.service.impl.GroupletService;
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
import org.jumpmind.symmetric.service.impl.SequenceService;
import org.jumpmind.symmetric.service.impl.StatisticService;
import org.jumpmind.symmetric.service.impl.TransformService;
import org.jumpmind.symmetric.service.impl.TransformService.TransformTableNodeGroupLink;
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
    
    protected IGroupletService groupletService;

    protected IStagingManager stagingManager;

    protected INodeCommunicationService nodeCommunicationService;
    
    protected IFileSyncService fileSyncService;    

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
    
    protected abstract SecurityServiceType getSecurityServiceType();

    protected void init() {
        this.propertiesFactory = createTypedPropertiesFactory();
        this.securityService = SecurityServiceFactory.create(getSecurityServiceType(), propertiesFactory.reload());
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
        this.nodeService = new NodeService(parameterService, symmetricDialect, securityService);
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
        this.groupletService = new GroupletService(this);
        this.triggerRouterService = new TriggerRouterService(this);
        this.outgoingBatchService = new OutgoingBatchService(parameterService, symmetricDialect,
                nodeService, configurationService, sequenceService, clusterService);
        this.dataService = new DataService(this);
        this.routerService = buildRouterService();
        this.nodeCommunicationService = buildNodeCommunicationService(clusterService, nodeService, parameterService, symmetricDialect);
        this.dataExtractorService = new DataExtractorService(parameterService, symmetricDialect,
                outgoingBatchService, routerService, configurationService, triggerRouterService,
                nodeService, dataService, transformService, statisticManager, stagingManager, clusterService, nodeCommunicationService);
        this.incomingBatchService = new IncomingBatchService(parameterService, symmetricDialect, clusterService);
        this.transportManager = new TransportManagerFactory(this).create();
        this.dataLoaderService = new DataLoaderService(this);
        this.registrationService = new RegistrationService(parameterService, symmetricDialect,
                nodeService, dataExtractorService, dataService, dataLoaderService,
                transportManager, statisticManager, configurationService);
        this.acknowledgeService = new AcknowledgeService(parameterService, symmetricDialect,
                outgoingBatchService, registrationService, stagingManager, this);
        this.pushService = new PushService(parameterService, symmetricDialect,
                dataExtractorService, acknowledgeService, transportManager, nodeService,
                clusterService, nodeCommunicationService, statisticManager);
        this.pullService = new PullService(parameterService, symmetricDialect, nodeService,
                dataLoaderService, registrationService, clusterService, nodeCommunicationService);
        this.fileSyncService = new FileSyncService(this);
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
            parameterService.setDatabaseHasBeenInitialized(true);
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
        if (!starting && !started) {
            try {
                starting = true;
                symmetricDialect.verifyDatabaseIsCompatible();
                setup();
                if (isConfigured()) {
                    Node node = nodeService.findIdentity();
                    if (node != null) {
                        log.info(
                                "Starting registered node [group={}, id={}, externalId={}]",
                                new Object[] { node.getNodeGroupId(), node.getNodeId(),
                                        node.getExternalId() });

                        triggerRouterService.syncTriggers();

                        if (Version.isOlderVersion(node.getSymmetricVersion())
                                && !parameterService.isRegistrationServer()
                                && parameterService.is(
                                        ParameterConstants.AUTO_RELOAD_SYM_ON_UPGRADE, true)) {
                            log.info("Minor version of SymmetricDS has increased.  Requesting a reload of key configuration tables");
                            String parentNodeId = node.getCreatedAtNodeId();
                            List<String> tableNames = new ArrayList<String>();
                            String tablePrefix = getTablePrefix();
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_PARAMETER));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_CHANNEL));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_TRIGGER));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_ROUTER));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_TRIGGER_ROUTER));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_TRANSFORM_TABLE));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_LOAD_FILTER));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_TRANSFORM_COLUMN));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_CONFLICT));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_GROUPLET));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_GROUPLET_LINK));
                            tableNames.add(TableConstants.getTableName(tablePrefix,
                                    TableConstants.SYM_TRIGGER_ROUTER_GROUPLET));

                            for (String tableName : tableNames) {
                                TableReloadRequest request = new TableReloadRequest();
                                request.setSourceNodeId(parentNodeId);
                                request.setTargetNodeId(node.getNodeId());
                                request.setTriggerId(tableName);
                                request.setRouterId(Constants.UNKNOWN_ROUTER_ID);
                                request.setLastUpdateBy(node.getSymmetricVersion() + " to "
                                        + Version.version());
                                dataService.saveTableReloadRequest(request);
                            }

                        }

                        if (parameterService
                                .is(ParameterConstants.HEARTBEAT_SYNC_ON_STARTUP, false) || StringUtils.isBlank(node.getDatabaseType()) ||
                                ! node.getSyncUrl().equals(parameterService.getSyncUrl())) {
                            heartbeat(false);
                        }

                    } else {
                        log.info("Starting unregistered node [group={}, externalId={}]",
                                parameterService.getNodeGroupId(), parameterService.getExternalId());
                    }

                    if (startJobs && jobManager != null) {
                        jobManager.startJobs();
                    }
                    log.info("Started SymmetricDS");
                    lastRestartTime = new Date();
                    started = true;

                } else {
                    log.warn("Did not start SymmetricDS.  It has not been configured properly");
                }
            } catch (Throwable ex) {
                log.error("An error occurred while starting SymmetricDS", ex);
            } finally {
                starting = false;
            }
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
            
            Table table = platform.readTableFromDatabase(null, null, TableConstants.getTableName(parameterService.getTablePrefix(), TableConstants.SYM_TRIGGER_ROUTER));            
            if (table != null) {
                
                List<Grouplet> grouplets = groupletService.getGrouplets(true);
                for (Grouplet grouplet : grouplets) {
                    groupletService.deleteGrouplet(grouplet);
                }
                
                List<TriggerRouter> triggerRouters = triggerRouterService.getTriggerRouters();
                for (TriggerRouter triggerRouter : triggerRouters) {
                    triggerRouterService.deleteTriggerRouter(triggerRouter);
                }

                for (TriggerRouter triggerRouter : triggerRouters) {
                    triggerRouterService.deleteTrigger(triggerRouter.getTrigger());
                    triggerRouterService.deleteRouter(triggerRouter.getRouter());
                }
            }
            
            table = platform.readTableFromDatabase(null, null, TableConstants.getTableName(
                    parameterService.getTablePrefix(), TableConstants.SYM_CONFLICT));
            if (table != null) {
                // need to remove all conflicts before we can remove the node
                // group links
                List<ConflictNodeGroupLink> conflicts = dataLoaderService
                        .getConflictSettingsNodeGroupLinks();
                for (ConflictNodeGroupLink conflict : conflicts) {
                    dataLoaderService.delete(conflict);
                }
            }

            table = platform.readTableFromDatabase(null, null, TableConstants.getTableName(
                    parameterService.getTablePrefix(), TableConstants.SYM_TRANSFORM_TABLE));
            if (table != null) {
                // need to remove all transforms before we can remove the node
                // group links
                List<TransformTableNodeGroupLink> transforms = transformService
                        .getTransformTables();
                for (TransformTableNodeGroupLink transformTable : transforms) {
                    transformService.deleteTransformTable(transformTable.getTransformId());
                }
            }
            
            table = platform.readTableFromDatabase(null, null, TableConstants.getTableName(
                    parameterService.getTablePrefix(), TableConstants.SYM_ROUTER));
            if (table != null) {
                List<Router> objects = triggerRouterService.getRouters();
                for (Router router : objects) {
                    triggerRouterService.deleteRouter(router);
                }
            }
            
            table = platform.readTableFromDatabase(null, null, TableConstants.getTableName(
                    parameterService.getTablePrefix(), TableConstants.SYM_CONFLICT));
            if (table != null) {
                List<ConflictNodeGroupLink> objects = dataLoaderService.getConflictSettingsNodeGroupLinks();
                for (ConflictNodeGroupLink obj : objects) {
                    dataLoaderService.delete(obj);
                }
            }
            
            table = platform.readTableFromDatabase(null, null, TableConstants.getTableName(
                    parameterService.getTablePrefix(), TableConstants.SYM_NODE_GROUP_LINK));
            if (table != null) {
                // remove the links so the symmetric table trigger will be
                // removed
                List<NodeGroupLink> links = configurationService.getNodeGroupLinks();
                for (NodeGroupLink nodeGroupLink : links) {
                    configurationService.deleteNodeGroupLink(nodeGroupLink);
                }
            }

            // this should remove all triggers because we have removed all the
            // trigger configuration
            triggerRouterService.syncTriggers();      
            
        } catch (SqlException ex) {
            log.warn("Error while trying remove triggers on tables", ex);
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
        log.warn("Removing node {}", nodeId);
        nodeService.deleteNode(nodeId, false);
        log.warn("Marking outgoing batch records as Ok for {}", nodeId);
        outgoingBatchService.markAllAsSentForNode(nodeId, true);
        log.warn("Marking incoming batch records as Ok for {}", nodeId);
        incomingBatchService.markIncomingBatchesOk(nodeId);
        log.warn("Done removing node {}", nodeId);        
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
        } else if (Constants.PLEASE_SET_ME.equals(registrationUrl)) {
            log.warn("Please set the registration.url for the node");
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getNodeGroupId())) {
            log.warn("Please set the group.id for the node");
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getExternalId())) {
            log.warn("Please set the external.id for the node");            
        } else if (node != null
                && (!node.getExternalId().equals(getParameterService().getExternalId()) || !node
                        .getNodeGroupId().equals(getParameterService().getNodeGroupId()))) {
            log.warn(
                    "The configured state does not match recorded database state.  The recorded external id is {} while the configured external id is {}. The recorded node group id is {} while the configured node group id is {}",
                    new Object[] { node.getExternalId(), getParameterService().getExternalId(),
                            node.getNodeGroupId(), getParameterService().getNodeGroupId() });
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
                log.debug("The current version of {} is newer than the last running version of {}", 
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
        getConfigurationService().clearCache();
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
    
    public IGroupletService getGroupletService() {
        return groupletService;
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
    
    public IFileSyncService getFileSyncService() {
        return fileSyncService;
    }

}
