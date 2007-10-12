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

import java.net.ConnectException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Date;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.Version;
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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.core.ConnectionCallback;
import org.springframework.jdbc.support.JdbcUtils;
import org.springframework.transaction.annotation.Transactional;

public class BootstrapService extends AbstractService implements IBootstrapService {

    static final Log logger = LogFactory.getLog(BootstrapService.class);

    private IDbDialect dbDialect;

    private String tablePrefix;

    private IParameterService parameterService;

    private IConfigurationService configurationService;

    private INodeService nodeService;

    private ITransportManager transportManager;

    private IDataLoaderService dataLoaderService;

    private RandomTimeSlot randomSleepTimeSlot;

    private boolean autoConfigureDatabase = true;

    private String insertNodeIntoDataSql;

    private String insertIntoDataEventSql;

    public void init() {
        this.randomSleepTimeSlot = new RandomTimeSlot(this.runtimeConfiguration, 60);
        if (autoConfigureDatabase) {
            logger.info("Initializing symmetric database.");
            dbDialect.initConfigDb(tablePrefix);
            populateDefautGlobalParametersIfNeeded();
            logger.info("Done initializing symmetric database.");
        } else {
            logger.info("Symmetric is not configured to auto create the database.");
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
        List<Trigger> triggers = configurationService.getInactiveTriggersForSourceNodeGroup(runtimeConfiguration
                .getNodeGroupId());
        for (Trigger trigger : triggers) {
            TriggerHistory history = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
            if (history != null) {
                logger.info("About to remove triggers for inactivated table: " + history.getSourceTableName());
                dbDialect.removeTrigger(history.getSourceSchemaName(), history.getNameForInsertTrigger());
                dbDialect.removeTrigger(history.getSourceSchemaName(), history.getNameForDeleteTrigger());
                dbDialect.removeTrigger(history.getSourceSchemaName(), history.getNameForUpdateTrigger());
                configurationService.inactivateTriggerHistory(history);
            } else {
                logger.info("A trigger was inactivated that had not yet been built.  Taking no action.");
            }
        }
    }

    private void updateOrCreateTriggers() {

        List<Trigger> triggers = configurationService.getActiveTriggersForSourceNodeGroup(runtimeConfiguration
                .getNodeGroupId());

        for (Trigger trigger : triggers) {

            try {
                TriggerReBuildReason reason = TriggerReBuildReason.NEW_TRIGGERS;

                Table table = dbDialect.getMetaDataFor(trigger.getSourceSchemaName(), trigger.getSourceTableName()
                        .toUpperCase(), false);

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

                    } else if (trigger.hasChangedSinceLastTriggerBuild(latestHistoryBeforeRebuild.getCreateTime())) {
                        reason = TriggerReBuildReason.TABLE_SYNC_CONFIGURATION_CHANGED;
                        forceRebuildOfTriggers = true;
                    }

                    // TODO should probably check to see if the time stamp on the symmetric-dialects.xml is newer than the
                    // create time on the audit record.

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
                            + trigger.getSourceTableName());
                }
            } catch (Exception ex) {
                logger.error("Failed to synchronize trigger for " + trigger.getSourceTableName(), ex);
            }

        }
    }

    public void register() {
        boolean registered = false;
        Node node = nodeService.findIdentity();
        if (node == null) {
            // If we cannot contact the server to register, we simply must wait and try again.   
            while (!registered) {
                try {
                    logger.info("Attempting to register with " + runtimeConfiguration.getRegistrationUrl());
                    registered = dataLoaderService.loadData(transportManager.getRegisterTransport(new Node(
                            this.runtimeConfiguration, dbDialect)));
                } catch (ConnectException e) {
                    logger.warn("Connection failed while registering.");
                } catch (Exception e) {
                    logger.error(e, e);
                }

                if (!registered) {
                    sleepBeforeRegistrationRetry();
                } else {
                    node = nodeService.findIdentity();
                    if (node != null) {
                        logger.info("Successfully registered node [id=" + node.getNodeId() + "]");
                    } else {
                        logger.error("Node registration is unavailable");
                    }
                }
            }
        } else {
            heartbeat();
        }
    }

    @Transactional
    public void heartbeat() {
        Node node = nodeService.findIdentity();
        if (node != null) {
            logger.info("Updating my node information and heartbeat time.");
            node.setHeartbeatTime(new Date());
            node.setDatabaseType(dbDialect.getName());
            node.setDatabaseVersion(dbDialect.getVersion());
            node.setSchemaVersion(runtimeConfiguration.getSchemaVersion());
            node.setExternalId(runtimeConfiguration.getExternalId());
            node.setNodeGroupId(runtimeConfiguration.getNodeGroupId());
            node.setSymmetricVersion(Version.VERSION);
            node.setSyncURL(runtimeConfiguration.getMyUrl());
            nodeService.updateNode(node);
            insertPushDataForNode(node);
            logger.info("Done updating my node information and heartbeat time.");
        }
    }

    /**
     * Because we can't add a trigger on the _node table, we are artificially generating heartbeat events.
     * @param node
     */
    private void insertPushDataForNode(Node node) {
        String whereClause = " t.node_id = '" + node.getNodeId() + "'";
        Trigger trig = configurationService.getTriggerFor(tablePrefix + "_node", runtimeConfiguration.getNodeGroupId());
        if (trig != null) {
            final String data = (String) jdbcTemplate.queryForObject(dbDialect.createCsvDataSql(trig, whereClause),
                    String.class);
            final String pk = (String) jdbcTemplate.queryForObject(dbDialect.createCsvPrimaryKeySql(trig, whereClause),
                    String.class);
            final TriggerHistory hist = configurationService.getLatestHistoryRecordFor(trig.getTriggerId());
            int dataId = (Integer) jdbcTemplate.execute(new ConnectionCallback() {
                public Object doInConnection(Connection c) throws SQLException, DataAccessException {
                    PreparedStatement pstmt = c.prepareStatement(insertNodeIntoDataSql, new int[] { 1 });
                    pstmt.setString(1, data);
                    pstmt.setString(2, pk);
                    pstmt.setInt(3, hist.getTriggerHistoryId());
                    pstmt.execute();
                    ResultSet rs = pstmt.getGeneratedKeys();
                    rs.next();
                    int dataId = rs.getInt(1);
                    JdbcUtils.closeResultSet(rs);
                    JdbcUtils.closeStatement(pstmt);
                    return dataId;
                }
            });

            List<Node> nodes = nodeService.findNodesToPushTo();
            for (Node node2 : nodes) {
                jdbcTemplate.update(insertIntoDataEventSql, new Object[] { dataId, node2.getNodeId() });
            }
        } else {
            logger
                    .info("Not generating data and data events for node because a trigger had not been created for that table yet.");
        }
    }

    private void sleepBeforeRegistrationRetry() {
        try {
            long sleepTimeInMs = DateUtils.MILLIS_PER_SECOND * randomSleepTimeSlot.getRandomValueSeededByDomainId();
            logger.warn("Could not register.  Sleeping for " + sleepTimeInMs + "ms before attempting again.");
            Thread.sleep(sleepTimeInMs);
        } catch (InterruptedException e) {
        }
    }

    /**
     * Need to make sure we are up to date with our table sync configuration of symmetric 
     * configuration tables.
     */
    protected void syncTableAuditConfigForConfigChannel() {
        List<String> tableNames = null;

        if (StringUtils.isEmpty(runtimeConfiguration.getRegistrationUrl())) {
            tableNames = configurationService.getRootConfigChannelTableNames();
        } else {
            tableNames = configurationService.getNodeConfigChannelTableNames();
        }
        configurationService.initSystemChannels();
        String groupId = runtimeConfiguration.getNodeGroupId();
        List<NodeGroupLink> targets = configurationService.getGroupLinksFor(groupId);
        if (targets != null && targets.size() > 0) {
            for (NodeGroupLink target : targets) {
                for (String tableName : tableNames) {
                    configurationService.initTriggerRowsForConfigChannel(tableName, groupId, target.getTargetGroupId());
                }
            }
        } else {
            logger.error("Could not find any targets for your group id of " + runtimeConfiguration.getNodeGroupId()
                    + ".  Please validate your node group id against the setup in the database.");
        }
    }

    private TriggerHistory rebuildTriggerIfNecessary(boolean forceRebuild, Trigger trigger, DataEventType dmlType,
            TriggerReBuildReason reason, TriggerHistory oldAudit, TriggerHistory audit, boolean create, Table table) {

        boolean triggerExists = false;

        TriggerHistory newTriggerHist = new TriggerHistory(table, trigger, reason, trigger
                .getTriggerName(DataEventType.INSERT), trigger.getTriggerName(DataEventType.UPDATE), trigger
                .getTriggerName(DataEventType.DELETE));

        String oldTriggerName = null;
        String oldSourceSchema = null;
        if (oldAudit != null) {
            oldTriggerName = oldAudit.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = oldAudit.getSourceSchemaName();
            triggerExists = dbDialect.doesTriggerExist(oldAudit.getSourceSchemaName(), oldAudit.getSourceTableName()
                    .toUpperCase(), oldTriggerName);
        } else {
            // We had no trigger_hist row, lets validate that the trigger as defined in the trigger
            // does not exist as well.
            oldTriggerName = newTriggerHist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = trigger.getSourceSchemaName();
            triggerExists = dbDialect.doesTriggerExist(trigger.getSourceSchemaName(), trigger.getSourceTableName()
                    .toUpperCase(), oldTriggerName);
        }

        if (!triggerExists && forceRebuild) {
            reason = TriggerReBuildReason.TRIGGERS_MISSING;
        }

        if ((forceRebuild || !create) && triggerExists) {
            dbDialect.removeTrigger(oldSourceSchema, oldTriggerName);
            triggerExists = false;
        }

        if (audit == null && (oldAudit == null || (!triggerExists && create))) {
            configurationService.insert(newTriggerHist);
            audit = configurationService.getLatestHistoryRecordFor(trigger.getTriggerId());
        }

        if (!triggerExists && create) {
            dbDialect.initTrigger(dmlType, trigger, audit, tablePrefix, table);
        }

        return audit;
    }

    private void populateDefautGlobalParametersIfNeeded() {
        parameterService.populateDefautGlobalParametersIfNeeded();
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
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

    public void setInsertNodeIntoDataSql(String insertNodeIntoDataSql) {
        this.insertNodeIntoDataSql = insertNodeIntoDataSql;
    }

    public void setInsertIntoDataEventSql(String insertIntoDataEventSql) {
        this.insertIntoDataEventSql = insertIntoDataEventSql;
    }

}
