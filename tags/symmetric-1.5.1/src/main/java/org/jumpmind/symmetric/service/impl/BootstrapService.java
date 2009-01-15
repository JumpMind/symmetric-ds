/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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
package org.jumpmind.symmetric.service.impl;

import java.io.File;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.service.IBootstrapService;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IRegistrationService;
import org.jumpmind.symmetric.service.IUpgradeService;
import org.jumpmind.symmetric.service.LockAction;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.transaction.annotation.Transactional;

public class BootstrapService extends AbstractService implements IBootstrapService {

    static final Log logger = LogFactory.getLog(BootstrapService.class);

    private IDbDialect dbDialect;

    private String tablePrefix;

    private IConfigurationService configurationService;

    private IClusterService clusterService;

    private INodeService nodeService;

    private IDataService dataService;

    private IUpgradeService upgradeService;

    private IRegistrationService registrationService;

    private List<Channel> defaultChannels;

    private boolean initialized = false;

    private Map<Integer,Trigger> triggerCache;
    
    public void setupDatabase() {
        setupDatabase(false);
    }
    
    private void autoConfigDatabase(boolean force) {
        if (parameterService.is(ParameterConstants.AUTO_CONFIGURE_DATABASE) || force) {
            logger.info("Initializing SymmetricDS database.");
            dbDialect.initConfigDb();
            if (defaultChannels != null) {
                logger.info("Setting up " + defaultChannels.size() + " default channels");
                for (Channel defaultChannel : defaultChannels) {
                    configurationService.saveChannel(defaultChannel);
                }
            }
            parameterService.rereadParameters();
            logger.info("Done initializing SymmetricDS database.");
        } else {
            logger.info("SymmetricDS is not configured to auto create the database.");
        }
    }
    
    public void setupDatabase(boolean force) {
        if (!initialized || force) {
            autoConfigDatabase(force);

            if (upgradeService.isUpgradeNecessary()) {
                if (parameterService.is(ParameterConstants.AUTO_UPGRADE)) {
                    try {
                        upgradeService.upgrade();
                        autoConfigDatabase(force);
                    } catch (RuntimeException ex) {
                        logger
                                .fatal(
                                        "The upgrade failed. The system may be unstable.  Please resolve the problem manually.",
                                        ex);
                        throw ex;
                    }
                } else {
                    throw new RuntimeException("Upgrade of node is necessary.  "
                            + "Please set auto.upgrade property to true for an automated upgrade.");
                }
            }
            initialized = true;

        }

        // lets do this every time init is called.
        clusterService.initLockTable();
    }

    /**
     * This is done periodically throughout the day (so it needs to be
     * efficient). If the trigger is created for the first time (no previous
     * trigger existed), then should we auto-resync data?
     */
    public void syncTriggers() {
        if (clusterService.lock(LockAction.SYNCTRIGGERS)) {
            try {
                logger.info("Synchronizing triggers");
                removeInactiveTriggers();
                updateOrCreateSymTriggers();
            } finally {
                clusterService.unlock(LockAction.SYNCTRIGGERS);
                logger.info("Done synchronizing triggers.");
            }
        }
    }

    private void removeInactiveTriggers() {
        List<Trigger> triggers = configurationService.getInactiveTriggersForSourceNodeGroup(parameterService
                .getString(ParameterConstants.NODE_GROUP_ID));
        for (Trigger trigger : triggers) {
            TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
            logger.info("About to remove triggers for inactivated table: " + history.getSourceTableName());
            if (history != null) {
                dbDialect.removeTrigger(history.getSourceCatalogName(), history.getSourceSchemaName(), history
                        .getNameForInsertTrigger(), trigger.getSourceTableName(), history);
                dbDialect.removeTrigger(history.getSourceCatalogName(), history.getSourceSchemaName(), history
                        .getNameForDeleteTrigger(), trigger.getSourceTableName(), history);
                dbDialect.removeTrigger(history.getSourceCatalogName(), history.getSourceSchemaName(), history
                        .getNameForUpdateTrigger(), trigger.getSourceTableName(), history);
                configurationService.inactivateTriggerHistory(history);
            } else {
                logger.info("A trigger was inactivated that had not yet been built.  Taking no action.");
            }
        }
    }

    /**
     * Create triggers on SymmetricDS tables so changes to configuration can be
     * synchronized.
     */
    private List<Trigger> getConfigurationTriggers() {
        List<Trigger> triggers = new ArrayList<Trigger>();
        Node me = nodeService.findIdentity();
        if (parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION) && me != null) {
            List<NodeGroupLink> links = configurationService.getGroupLinksFor(me.getNodeGroupId());
            for (NodeGroupLink nodeGroupLink : links) {
                if (nodeGroupLink.getDataEventAction().equals(DataEventAction.WAIT_FOR_POLL)) {
                    triggers.addAll(configurationService.getConfigurationTriggers(nodeGroupLink.getSourceGroupId(),
                            nodeGroupLink.getTargetGroupId(), false));
                }
            }
        } else {
            logger
                    .info("Auto syncing of configuration is currently off.  Configuration triggers will not be generated.");
        }
        return triggers;
    }
    
    public Map<Integer,Trigger> getCachedTriggers(boolean refreshCache) {
        if (triggerCache == null || refreshCache) {
            synchronized (this) {
                triggerCache = new HashMap<Integer, Trigger>();
                List<Trigger> triggers = new ArrayList<Trigger>();
                triggers.addAll(getConfigurationTriggers());
                triggers.addAll(configurationService.getActiveTriggersForSourceNodeGroup(parameterService
                        .getString(ParameterConstants.NODE_GROUP_ID)));
                for (Trigger trigger : triggers) {
                    triggerCache.put(trigger.getTriggerId(), trigger);
                }               
            }
        }
        return triggerCache;
    }

    

    private void updateOrCreateSymTriggers() {
        Collection<Trigger> triggers = getCachedTriggers(true).values();

        for (Trigger trigger : triggers) {

            String schemaPlusTriggerName = (trigger.getSourceSchemaName() != null ? trigger.getSourceSchemaName() + "."
                    : "")
                    + trigger.getSourceTableName();

            try {

                TriggerReBuildReason reason = TriggerReBuildReason.NEW_TRIGGERS;

                Table table = dbDialect.getMetaDataFor(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), false);

                if (table != null) {
                    TriggerHistory latestHistoryBeforeRebuild = configurationService.getLatestHistoryRecordFor(trigger
                            .getTriggerId());

                    boolean forceRebuildOfTriggers = false;
                    if (latestHistoryBeforeRebuild == null) {
                        reason = TriggerReBuildReason.NEW_TRIGGERS;
                        forceRebuildOfTriggers = true;

                    } else if (TriggerHistory.calculateTableHashFor(table) != latestHistoryBeforeRebuild.getTableHash()) {
                        reason = TriggerReBuildReason.TABLE_SCHEMA_CHANGED;
                        forceRebuildOfTriggers = true;

                    } else if (trigger.hasChangedSinceLastTriggerBuild(latestHistoryBeforeRebuild.getCreateTime())
                            || trigger.getHashedValue() != latestHistoryBeforeRebuild.getTriggerRowHash()) {
                        reason = TriggerReBuildReason.TABLE_SYNC_CONFIGURATION_CHANGED;
                        forceRebuildOfTriggers = true;
                    }

                    // TODO should probably check to see if the time stamp on
                    // the symmetric-dialects.xml is newer than the
                    // create time on the hist record.

                    TriggerHistory newestHistory = rebuildTriggerIfNecessary(forceRebuildOfTriggers, trigger,
                            DataEventType.DELETE, reason, latestHistoryBeforeRebuild, rebuildTriggerIfNecessary(
                                    forceRebuildOfTriggers, trigger, DataEventType.UPDATE, reason,
                                    latestHistoryBeforeRebuild, rebuildTriggerIfNecessary(forceRebuildOfTriggers,
                                            trigger, DataEventType.INSERT, reason, latestHistoryBeforeRebuild, null,
                                            trigger.isSyncOnInsert(), table), trigger.isSyncOnUpdate(), table), trigger
                                    .isSyncOnDelete(), table);

                    if (latestHistoryBeforeRebuild != null && newestHistory != null) {
                        configurationService.inactivateTriggerHistory(latestHistoryBeforeRebuild);
                    }

                } else {
                    logger.error("The configured table does not exist in the datasource that is configured: "
                            + schemaPlusTriggerName);
                }
            } catch (Exception ex) {
                logger.error("Failed to synchronize trigger for " + schemaPlusTriggerName, ex);
            }

        }
    }

    @Deprecated
    public void register() {
        validateConfiguration();
    }

    public void validateConfiguration() {
        Node node = nodeService.findIdentity();
        if (node == null && !configurationService.isRegistrationServer()) {
            if (!parameterService.is(ParameterConstants.START_PULL_JOB)) {
                registrationService.registerWithServer();
            }
        } else if (node != null && parameterService.getExternalId().equals(node.getExternalId())
                && parameterService.getNodeGroupId().equals(node.getNodeGroupId())) {
            heartbeat();
        } else if (node == null) {
            if (!loadFromScriptIfProvided()) {
                logger
                        .info("Could not find my identity in the database and this node is configured as a registration server.  We are auto inserting the required rows to begin operation.");
                // TODO
                // nodeService.insertIdentity();
            }
        } else {
            throw new IllegalStateException(
                    "The configured state does not match recorded database state.  The recorded external id is "
                            + node.getExternalId() + " while the configured external id is "
                            + parameterService.getExternalId() + ".  The recorded node group id is "
                            + node.getNodeGroupId() + " while the configured node group id is "
                            + parameterService.getNodeGroupId());
        }
    }

    /**
     * Give the end use the option to provide a script that will load a
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
                new SqlScript(fileUrl, jdbcTemplate.getDataSource(), true).execute();
                loaded = true;
            }
        }
        return loaded;
    }

    @Transactional
    public void heartbeat() {
        Node node = nodeService.findIdentity();
        if (node != null) {
            logger.info("Updating my node information and heartbeat time.");
            node.setHeartbeatTime(new Date());
            node.setTimezoneOffset(AppUtils.getTimezoneOffset());
            node.setDatabaseType(dbDialect.getName());
            node.setDatabaseVersion(dbDialect.getVersion());
            node.setSchemaVersion(parameterService.getString(ParameterConstants.SCHEMA_VERSION));
            node.setExternalId(parameterService.getExternalId());
            node.setNodeGroupId(parameterService.getNodeGroupId());
            node.setSymmetricVersion(Version.version());
            if (!StringUtils.isBlank(parameterService.getMyUrl())) {
                node.setSyncURL(parameterService.getMyUrl());
            } else {
                node.setSyncURL(Constants.PROTOCOL_NONE + "://" + AppUtils.getServerId());
            }
            nodeService.updateNode(node);
            logger.info("Done updating my node information and heartbeat time.");
            if (!configurationService.isRegistrationServer()) {
                dataService.insertHeartbeatEvent(node);
            }
        }
    }

    private TriggerHistory rebuildTriggerIfNecessary(boolean forceRebuild, Trigger trigger, DataEventType dmlType,
            TriggerReBuildReason reason, TriggerHistory oldhist, TriggerHistory hist, boolean create, Table table) {

        boolean triggerExists = false;

        TriggerHistory newTriggerHist = new TriggerHistory(table, trigger, reason);
        int maxTriggerNameLength = dbDialect.getMaxTriggerNameLength();
        String triggerPrefix = parameterService.getString(ParameterConstants.RUNTIME_CONFIG_TRIGGER_PREFIX);
        newTriggerHist.setNameForInsertTrigger(dbDialect.getTriggerName(DataEventType.INSERT, triggerPrefix, maxTriggerNameLength,
                trigger, hist).toUpperCase());
        newTriggerHist.setNameForUpdateTrigger(dbDialect.getTriggerName(DataEventType.UPDATE, triggerPrefix, maxTriggerNameLength,
                trigger, hist).toUpperCase());
        newTriggerHist.setNameForDeleteTrigger(dbDialect.getTriggerName(DataEventType.DELETE, triggerPrefix, maxTriggerNameLength,
                trigger, hist).toUpperCase());
        
        String oldTriggerName = null;
        String oldSourceSchema = null;
        String oldCatalogName = null;
        if (oldhist != null) {
            oldTriggerName = oldhist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = oldhist.getSourceSchemaName();
            oldCatalogName = oldhist.getSourceCatalogName();
            triggerExists = dbDialect.doesTriggerExist(oldCatalogName, oldSourceSchema, oldhist.getSourceTableName(),
                    oldTriggerName);
        } else {
            // We had no trigger_hist row, lets validate that the trigger as
            // defined in the trigger
            // does not exist as well.
            oldTriggerName = newTriggerHist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = trigger.getSourceSchemaName();
            oldCatalogName = trigger.getSourceCatalogName();
            triggerExists = dbDialect.doesTriggerExist(oldCatalogName, oldSourceSchema, trigger.getSourceTableName(),
                    oldTriggerName);
        }

        if (!triggerExists && forceRebuild) {
            reason = TriggerReBuildReason.TRIGGERS_MISSING;
        }

        if ((forceRebuild || !create) && triggerExists) {
            dbDialect.removeTrigger(oldCatalogName, oldSourceSchema, oldTriggerName, trigger.getSourceTableName(), oldhist);
            triggerExists = false;
        }

        boolean isDeadTrigger = !trigger.isSyncOnInsert() && !trigger.isSyncOnUpdate() && !trigger.isSyncOnDelete();

        if (hist == null && (oldhist == null || (!triggerExists && create) || (isDeadTrigger && forceRebuild))) {
            configurationService.insert(newTriggerHist);
            hist = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        }

        if (!triggerExists && create) {
            dbDialect.initTrigger(dmlType, trigger, hist, tablePrefix, table);
        }

        return hist;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setDataService(IDataService dataService) {
        this.dataService = dataService;
    }

    public void setUpgradeService(IUpgradeService upgradeService) {
        this.upgradeService = upgradeService;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setRegistrationService(IRegistrationService registrationService) {
        this.registrationService = registrationService;
    }

    public void setDefaultChannels(List<Channel> defaultChannels) {
        this.defaultChannels = defaultChannels;
    }

}
