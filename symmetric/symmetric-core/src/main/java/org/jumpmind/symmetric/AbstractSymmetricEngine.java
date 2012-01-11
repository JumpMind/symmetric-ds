package org.jumpmind.symmetric;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.log.Log;
import org.jumpmind.log.LogFactory;
import org.jumpmind.properties.TypedProperties;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.DeploymentType;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.ext.IExtensionPointManager;
import org.jumpmind.symmetric.io.DefaultOfflineClientListener;
import org.jumpmind.symmetric.io.IOfflineClientListener;
import org.jumpmind.symmetric.job.DefaultOfflineServerListener;
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
import org.jumpmind.symmetric.service.impl.NodeService;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService;
import org.jumpmind.symmetric.service.impl.ParameterService;
import org.jumpmind.symmetric.service.impl.PullService;
import org.jumpmind.symmetric.service.impl.PurgeService;
import org.jumpmind.symmetric.service.impl.PushService;
import org.jumpmind.symmetric.service.impl.RegistrationService;
import org.jumpmind.symmetric.service.impl.RouterService;
import org.jumpmind.symmetric.service.impl.SecurityService;
import org.jumpmind.symmetric.service.impl.StatisticService;
import org.jumpmind.symmetric.service.impl.TransformService;
import org.jumpmind.symmetric.service.impl.TriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticManager;
import org.jumpmind.symmetric.transport.ConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.IConcurrentConnectionManager;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.transport.TransportManagerFactory;
import org.jumpmind.symmetric.util.AppUtils;

abstract public class AbstractSymmetricEngine implements ISymmetricEngine {

    private static Map<String, ISymmetricEngine> registeredEnginesByUrl = new HashMap<String, ISymmetricEngine>();
    private static Map<String, ISymmetricEngine> registeredEnginesByName = new HashMap<String, ISymmetricEngine>();

    protected Log log = LogFactory.getLog(getClass());

    private boolean started = false;

    private boolean starting = false;

    private boolean setup = false;

    protected ITypedPropertiesFactory propertiesFactory;

    protected IDatabasePlatform platform;

    protected ISecurityService securityService;

    protected IParameterService parameterService;

    protected ISymmetricDialect symmetricDialect;

    protected INodeService nodeService;

    protected IConfigurationService configurationService;

    protected IBandwidthService bandwidthService;

    protected DeploymentType deploymentType;

    protected IStatisticService statisticService;

    protected IStatisticManager statisticManager;

    protected IConcurrentConnectionManager concurrentConnectionManager;

    protected ITransportManager transportManager;

    protected IClusterService clusterService;

    protected IPurgeService purgeService;

    protected ITransformService transformService;

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

    protected IExtensionPointManager extensionPointManger;

    abstract protected ITypedPropertiesFactory createTypedPropertiesFactory();

    abstract protected IDatabasePlatform createDatabasePlatform(TypedProperties properties);

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
            return registeredEnginesByName.get(name);
        } else {
            return null;
        }
    }

    protected void init() {
        this.deploymentType = new DeploymentType();
        this.securityService = createSecurityService();
        this.propertiesFactory = createTypedPropertiesFactory();
        TypedProperties properties = this.propertiesFactory.reload();
        log = LogFactory.getLog("org.jumpmind."
                + properties.get(ParameterConstants.ENGINE_NAME, "default"));
        this.parameterService = new ParameterService(platform, propertiesFactory, properties.get(
                ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX, "sym"), log);

        this.platform = createDatabasePlatform(properties);
        this.platform.setDelimitedIdentifierModeOn(parameterService
                .is(ParameterConstants.DB_FORCE_DELIMITED_IDENTIFIER_ON));
        this.platform.setDelimitedIdentifierModeOn(!parameterService
                .is(ParameterConstants.DB_FORCE_DELIMITED_IDENTIFIER_OFF));
        this.platform.setClearCacheModelTimeoutInMs(parameterService
                .getLong(ParameterConstants.CACHE_TIMEOUT_TABLES_IN_MS));

        this.bandwidthService = new BandwidthService(parameterService, log);
        this.symmetricDialect = createSymmetricDialect();
        this.nodeService = new NodeService(parameterService, symmetricDialect);
        this.configurationService = new ConfigurationService(parameterService, symmetricDialect,
                nodeService);
        this.statisticService = new StatisticService(parameterService, symmetricDialect);
        this.statisticManager = new StatisticManager(parameterService, nodeService,
                configurationService, statisticService);
        this.concurrentConnectionManager = new ConcurrentConnectionManager(parameterService,
                statisticManager);
        this.clusterService = new ClusterService(parameterService, symmetricDialect);
        this.purgeService = new PurgeService(parameterService, symmetricDialect, clusterService,
                statisticManager);
        this.transformService = new TransformService(parameterService, symmetricDialect);
        this.triggerRouterService = new TriggerRouterService(parameterService, symmetricDialect,
                clusterService, configurationService, statisticManager);
        this.outgoingBatchService = new OutgoingBatchService(parameterService, symmetricDialect,
                nodeService, configurationService);
        this.dataService = new DataService(parameterService, symmetricDialect, deploymentType,
                triggerRouterService, nodeService, purgeService, configurationService,
                outgoingBatchService, statisticManager);
        this.routerService = new RouterService(parameterService, symmetricDialect, clusterService,
                dataService, configurationService, triggerRouterService, outgoingBatchService,
                nodeService, statisticManager, transformService);
        this.dataExtractorService = new DataExtractorService(parameterService, symmetricDialect,
                outgoingBatchService, routerService, configurationService, triggerRouterService,
                nodeService, statisticManager);
        this.incomingBatchService = new IncomingBatchService(parameterService, symmetricDialect);
        this.transportManager = new TransportManagerFactory(this).create();
        this.dataLoaderService = new DataLoaderService(parameterService, symmetricDialect,
                incomingBatchService, configurationService, transportManager, statisticManager,
                nodeService, transformService, triggerRouterService);
        this.registrationService = new RegistrationService(parameterService, symmetricDialect,
                nodeService, dataExtractorService, dataService, dataLoaderService,
                transportManager, statisticManager);
        this.acknowledgeService = new AcknowledgeService(parameterService, symmetricDialect,
                outgoingBatchService, registrationService);
        this.pushService = new PushService(parameterService, symmetricDialect,
                dataExtractorService, acknowledgeService, transportManager, nodeService,
                clusterService);
        this.pullService = new PullService(parameterService, symmetricDialect, nodeService,
                dataLoaderService, registrationService, clusterService);
        this.jobManager = createJobManager();

        this.nodeService.addOfflineServerListener(new DefaultOfflineServerListener(log,
                statisticManager, nodeService, outgoingBatchService));

        IOfflineClientListener defaultlistener = new DefaultOfflineClientListener(log,
                parameterService, nodeService);
        this.pullService.addOfflineListener(defaultlistener);
        this.pushService.addOfflineListener(defaultlistener);

        this.extensionPointManger = createExtensionPointManager();
        this.extensionPointManger.register();

        registerWithJMX();

        registerHandleToEngine();

    }

    protected static ISecurityService createSecurityService() {
        // TODO check system property. integrate eric's changes
        return new SecurityService();
    }
    
    abstract protected ISymmetricDialect createSymmetricDialect();

    abstract protected IJobManager createJobManager();

    abstract protected void registerWithJMX();

    abstract protected IExtensionPointManager createExtensionPointManager();

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
        AppUtils.cleanupTempFiles();
        getParameterService().rereadParameters();
        if (!setup) {
            setupDatabase(false);
            setup = true;
        }
    }

    public void setupDatabase(boolean force) {
        configurationService.autoConfigDatabase(force);
        clusterService.initLockTable();
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
                        log.info("Starting registered node [group=%s, id=%s, externalId=%s]",
                                node.getNodeGroupId(), node.getNodeId(), node.getExternalId());
                    } else {
                        log.info("Starting unregistered node [group=%s, externalId=%s]",
                                parameterService.getNodeGroupId(), parameterService.getExternalId());
                    }
                    triggerRouterService.syncTriggers();
                    heartbeat(false);
                    if (startJobs) {
                        jobManager.startJobs();
                    }
                    log.info("Started SymmetricDS");
                    started = true;
                } finally {
                    starting = false;
                }

            }
        } else {
            log.warn("Did not start SymmetricDS.  It has not been configured properly");
        }

        log.info(
                "SymmetricDS: type=%s, name=%s, version=%s, groupId=%s, externalId=%s, databaseName=%s, databaseVersion=%s, driverName=%s, driverVersion=%s",
                getDeploymentType().getDeploymentType(), getEngineName(), Version.version(),
                getParameterService().getNodeGroupId(), getParameterService().getExternalId(),
                symmetricDialect.getName(), symmetricDialect.getVersion(),
                symmetricDialect.getDriverName(), symmetricDialect.getDriverVersion());
        return started;
    }

    public synchronized void stop() {
        log.info("SymmetricDSClosing", getParameterService().getExternalId(), Version.version(),
                symmetricDialect.getName());
        if (jobManager != null) {
            jobManager.stopJobs();
        }
        routerService.stop();
        started = false;
        starting = false;
    }

    public synchronized void destroy() {
        stop();
        if (jobManager != null) {
            jobManager.destroy();
        }
        getRouterService().destroy();
        removeMeFromMap(registeredEnginesByName);
        removeMeFromMap(registeredEnginesByUrl);
    }

    public String reloadNode(String nodeId) {
        return dataService.reloadNode(nodeId);
    }

    public String sendSQL(String nodeId, String catalogName, String schemaName, String tableName,
            String sql) {
        return dataService.sendSQL(nodeId, catalogName, schemaName, tableName, sql, false);
    }

    public RemoteNodeStatuses push() {
        return pushService.pushData();
    }

    public void syncTriggers() {
        triggerRouterService.syncTriggers();
    }

    public NodeStatus getNodeStatus() {
        return nodeService.getNodeStatus();
    }

    public RemoteNodeStatuses pull() {
        return pullService.pullData();
    }

    public void route() {
        routerService.routeData();
    }

    public void purge() {
        purgeService.purgeOutgoing();
        purgeService.purgeIncoming();
        purgeService.purgeDataGaps();
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
                    "Please set the property %s so this node may pull registration or manually insert configuration into the configuration tables",
                    ParameterConstants.REGISTRATION_URL);
        } else if (Constants.PLEASE_SET_ME.equals(getParameterService().getExternalId())
                || Constants.PLEASE_SET_ME.equals(registrationUrl)
                || Constants.PLEASE_SET_ME.equals(getParameterService().getNodeGroupId())) {
            log.warn("Please set the registration.url, node.group.id, and external.id for the node");
        } else if (node != null
                && (!node.getExternalId().equals(getParameterService().getExternalId()) || !node
                        .getNodeGroupId().equals(getParameterService().getNodeGroupId()))) {
            log.warn(
                    "The configured state does not match recorded database state.  The recorded external id is %s while the configured external id is %s. The recorded node group id is %s while the configured node group id is %s",
                    node.getExternalId(), getParameterService().getExternalId(),
                    node.getNodeGroupId(), getParameterService().getNodeGroupId());

        } else if (node != null && StringUtils.isBlank(getParameterService().getRegistrationUrl())
                && StringUtils.isBlank(getParameterService().getSyncUrl())) {
            log.warn("The sync.url property must be set for the registration server.  Otherwise, registering nodes will not be able to sync with it");

        } else if (offlineNodeDetectionPeriodSeconds > 0
                && offlineNodeDetectionPeriodSeconds <= heartbeatSeconds) {
            // Offline node detection is not disabled (-1) and the value is too
            // small (less than the heartbeat)
            log.warn(
                    "The %s property must be a longer period of time than the %s property.  Otherwise, nodes will be taken offline before the heartbeat job has a chance to run",
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
        dataService.heartbeat(force);
    }

    public void openRegistration(String nodeGroupId, String externalId) {
        registrationService.openRegistration(nodeGroupId, externalId);
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

    public DeploymentType getDeploymentType() {
        return deploymentType;
    }

    public ITransformService getTransformService() {
        return this.transformService;
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
            throw new IllegalStateException(
                    "Could not register engine.  There was already an engine registered under the url: "
                            + getSyncUrl());
        }

        alreadyRegister = registeredEnginesByName.get(getEngineName());
        if (alreadyRegister == null || alreadyRegister.equals(this)) {
            registeredEnginesByName.put(getEngineName(), this);
        } else {
            throw new IllegalStateException(
                    "Could not register engine.  There was already an engine registered under the name: "
                            + getEngineName());
        }

    }

    public Log getLog() {
        return log;
    }

}
