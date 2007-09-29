package org.jumpmind.symmetric.service.impl;

import java.io.IOException;
import java.net.ConnectException;
import java.util.List;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataLoaderService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.transport.ITransportManager;
import org.jumpmind.symmetric.util.RandomTimeSlot;

public class BootstrapService extends AbstractService implements
        IBootstrapService {

    static final Log logger = LogFactory.getLog(BootstrapService.class);

    private IDbDialect dbDialect;

    private String tablePrefix;

    private IParameterService parameterService;

    private IConfigurationService configurationService;

    private INodeService nodeService;

    private ITransportManager transportManager;

    private IDataLoaderService dataLoaderService;

    private boolean autoConfigureDatabase = true;

    public void init() {
        if (autoConfigureDatabase) {
            logger.info("Initializing symmetric database.");
            dbDialect.initConfigDb(tablePrefix);
            populateDefautGlobalParametersIfNeeded();
            logger.info("Done initializing symmetric database.");
        } else {
            logger
                    .info("Symmetric is not configured to auto create the database.");
        }
    }

    /**
     * This is done periodically throughout the day (so it needs to be efficient).  If the trigger
     * is created for the first time (no previous trigger existed), then
     * should we auto-resync data?
     */
    public void syncTriggers() {
        logger.info("Synchronizing triggers.");
        syncTableAuditConfigForConfigChannel();
        removeInactiveTriggers();
        updateOrCreateTriggers();
        logger.info("Done synchronizing triggers.");
    }

    private void removeInactiveTriggers() {
        List<Trigger> triggers = configurationService
                .getInactiveTriggersForSourceNodeGroup(runtimeConfiguration
                        .getNodeGroupId());
        for (Trigger trigger : triggers) {
            TriggerHistory history = configurationService
                    .getLatestHistoryRecordFor(trigger.getTriggerId());
            if (history != null) {
                logger.info("About to remove triggers for inactivated table: "
                        + history.getSourceTableName());
                dbDialect.removeTrigger(history.getSourceSchemaName(), history
                        .getNameForInsertTrigger());
                dbDialect.removeTrigger(history.getSourceSchemaName(), history
                        .getNameForDeleteTrigger());
                dbDialect.removeTrigger(history.getSourceSchemaName(), history
                        .getNameForUpdateTrigger());
                configurationService.inactivateTriggerHistory(history);
            } else {
                logger
                        .info("A trigger was inactivated that had not yet been built.  Taking no action.");
            }
        }
    }

    private void updateOrCreateTriggers() {

        List<Trigger> triggers = configurationService
                .getActiveTriggersForSourceNodeGroup(runtimeConfiguration
                        .getNodeGroupId());

        for (Trigger trigger : triggers) {

            TriggerReBuildReason reason = TriggerReBuildReason.NEW_TRIGGERS;

            Table table = dbDialect.getMetaDataFor(trigger
                    .getSourceSchemaName(), trigger.getSourceTableName()
                    .toUpperCase(), false);

            if (table != null) {
                TriggerHistory latestHistoryBeforeRebuild = configurationService
                        .getLatestHistoryRecordFor(trigger.getTriggerId());

                boolean forceRebuildOfTriggers = false;
                if (latestHistoryBeforeRebuild == null) {
                    reason = TriggerReBuildReason.NEW_TRIGGERS;
                    forceRebuildOfTriggers = true;

                } else if (TriggerHistory.calculateTableHashFor(table) != latestHistoryBeforeRebuild
                        .getTableHash()) {
                    reason = TriggerReBuildReason.TABLE_SCHEMA_CHANGED;
                    forceRebuildOfTriggers = true;

                } else if (trigger
                        .hasChangedSinceLastTriggerBuild(latestHistoryBeforeRebuild
                                .getCreateTime())) {
                    reason = TriggerReBuildReason.TABLE_SYNC_CONFIGURATION_CHANGED;
                    forceRebuildOfTriggers = true;
                }
                // TODO should probably check to see if the time stamp on the symmetric-dialects.xml is newer than the
                // create time on the audit record.

                TriggerHistory newestHistory = rebuildTriggerIfNecessary(
                        forceRebuildOfTriggers, trigger, DataEventType.DELETE,
                        reason, latestHistoryBeforeRebuild,
                        rebuildTriggerIfNecessary(forceRebuildOfTriggers,
                                trigger, DataEventType.UPDATE, reason,
                                latestHistoryBeforeRebuild,
                                rebuildTriggerIfNecessary(
                                        forceRebuildOfTriggers, trigger,
                                        DataEventType.INSERT, reason,
                                        latestHistoryBeforeRebuild, null,
                                        trigger.isSyncOnInsert(), table),
                                trigger.isSyncOnUpdate(), table), trigger
                                .isSyncOnDelete(), table);

                if (latestHistoryBeforeRebuild != null && newestHistory != null) {
                    configurationService
                            .inactivateTriggerHistory(latestHistoryBeforeRebuild);
                }

            } else {
                // TODO do we need to log configuration errors to a common place?
                logger
                        .error("The configured table does not exist in the datasource that is configured: "
                                + trigger.getSourceTableName());
            }
        }
    }

    public void register() {
        boolean registered = false;
        Node client = nodeService.findIdentity();
        if (client == null) {
            // If we cannot contact the server to register, we simply must wait and try again.   
            while (!registered) {
                try {
                    registered = dataLoaderService.loadData(transportManager
                            .getRegisterTransport(new Node(this.runtimeConfiguration,
                                    dbDialect)));
                } catch (ConnectException e) {
                    logger.warn("Connection failed while registering.");
                } catch (IOException e) {
                    logger.error(e, e);
                }

                if (!registered) {
                    sleepBeforeRegistrationRetry();
                } else {
                    logger.info("Successfully registered.");
                }
            }
        } else {
            // TODO - check in with the server to update schema version numbers
        }
    }

    private void sleepBeforeRegistrationRetry() {
        RandomTimeSlot ts = new RandomTimeSlot(this.runtimeConfiguration, 60);
        try {
            long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND
                    * ts.getRandomValueSeededByDomainId();
            logger.warn("Could not register.  Sleeping for " + sleepTimeInMs
                    + "ms before attempting again.");
            Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Need to make sure we are up to date with our table sync configuration of symmetric 
     * configuration tables.
     */
    protected void syncTableAuditConfigForConfigChannel() {
        if (runtimeConfiguration.getRegistrationUrl() == null
                || runtimeConfiguration.getRegistrationUrl().trim().equals("")) {
            configurationService.initConfigChannel();
            String domainName = runtimeConfiguration.getNodeGroupId();
            List<NodeGroupLink> targets = configurationService
                    .getGroupLinksFor(domainName);
            List<String> tableNames = configurationService
                    .getConfigChannelTableNames();
            if (targets != null && targets.size() > 0) {
                for (NodeGroupLink target : targets) {
                    for (String tableName : tableNames) {
                        configurationService.initTriggersForConfigTables(
                                tableName, domainName, target
                                        .getTargetGroupId());
                    }
                }
            } else {
                logger
                        .error("Could not find any targets for your group id of "
                                + runtimeConfiguration.getNodeGroupId()
                                + ".  Please validate your node group id against the setup in the database.");
            }
        }
    }

    private TriggerHistory rebuildTriggerIfNecessary(boolean forceRebuild,
            Trigger trigger, DataEventType dmlType,
            TriggerReBuildReason reason, TriggerHistory oldAudit,
            TriggerHistory audit, boolean create, Table table) {

        boolean triggerExists = false;

        TriggerHistory newTriggerHist = new TriggerHistory(table, trigger,
                reason, trigger.getTriggerName(DataEventType.INSERT), trigger
                        .getTriggerName(DataEventType.UPDATE), trigger
                        .getTriggerName(DataEventType.DELETE));

        String oldTriggerName = null;
        String oldSourceSchema = null;
        if (oldAudit != null) {
            oldTriggerName = oldAudit.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = oldAudit.getSourceSchemaName();
            triggerExists = dbDialect.doesTriggerExist(oldAudit
                    .getSourceSchemaName(), oldAudit.getSourceTableName()
                    .toUpperCase(), oldTriggerName);
        } else {
            // We had no trigger_hist row, lets validate that the trigger as defined in the trigger
            // does not exist as well.
            oldTriggerName = newTriggerHist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = trigger.getSourceSchemaName();
            triggerExists = dbDialect.doesTriggerExist(trigger
                    .getSourceSchemaName(), trigger.getSourceTableName()
                    .toUpperCase(), oldTriggerName);
        }

        if (!triggerExists && forceRebuild) {
            logger.info("Did not find trigger.  Rebuilding.");
            reason = TriggerReBuildReason.TRIGGERS_MISSING;
        }

        if ((forceRebuild || !create) && triggerExists) {
            dbDialect.removeTrigger(oldSourceSchema, oldTriggerName);
            triggerExists = false;
        }

        if (!triggerExists && create) {
            if (audit == null) {
                configurationService.insert(newTriggerHist);
                audit = configurationService.getLatestHistoryRecordFor(trigger
                        .getTriggerId());
            }
            // TODO: fix node table trigger which cannot select itself
            if (!trigger.getSourceTableName().endsWith("node")) {
                logger.info("Creating " + dmlType.toString() + " trigger for "
                        + trigger.getSourceTableName());
                dbDialect.initTrigger(dmlType, trigger, audit, tablePrefix,
                        table);
            }
        }

        return audit;
    }

    private void populateDefautGlobalParametersIfNeeded() {
        parameterService.populateDefautGlobalParametersIfNeeded();
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setConfigurationService(
            IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public boolean isAutoConfigureDatabase() {
        return autoConfigureDatabase;
    }

    public void setAutoConfigureDatabase(boolean autoConfigureDatabase) {
        this.autoConfigureDatabase = autoConfigureDatabase;
    }

    public void setNodeService(INodeService clientService) {
        this.nodeService = clientService;
    }

    public void setTransportManager(ITransportManager transportManager) {
        this.transportManager = transportManager;
    }

    public void setDataLoaderService(IDataLoaderService dataLoaderService) {
        this.dataLoaderService = dataLoaderService;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

}
