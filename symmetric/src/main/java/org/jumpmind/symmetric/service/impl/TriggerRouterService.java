/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
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

import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.config.ITriggerCreationListener;
import org.jumpmind.symmetric.config.TriggerFailureListener;
import org.jumpmind.symmetric.config.TriggerSelector;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Router;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.service.LockActionConstants;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

public class TriggerRouterService extends AbstractService implements ITriggerRouterService {

    private List<String> rootConfigChannelTableNames;

    private Map<String, String> rootConfigChannelInitialLoadSelect;

    private IClusterService clusterService;

    private IConfigurationService configurationService;

    private Map<Integer, List<TriggerRouter>> triggerCache;

    private List<ITriggerCreationListener> triggerCreationListeners;

    private TriggerFailureListener failureListener = new TriggerFailureListener();

    /**
     * Cache the history for performance. History never changes and does not grow big so this should be OK.
     */
    private HashMap<Integer, TriggerHistory> historyMap = new HashMap<Integer, TriggerHistory>();

    public TriggerRouterService() {
        this.addTriggerCreationListeners(this.failureListener);
    }

    public void inactivateTriggerHistory(TriggerHistory history) {
        jdbcTemplate.update(getSql("inactivateTriggerHistorySql"), new Object[] { history.getTriggerHistoryId() });
    }

    public List<String> getRootConfigChannelTableNames() {
        return rootConfigChannelTableNames;
    }

    public List<TriggerRouter> getRegistrationTriggers(String sourceGroupId, String targetGroupId) {
        return getConfigurationTriggers(sourceGroupId, targetGroupId);
    }

    protected List<TriggerRouter> getConfigurationTriggers(String sourceGroupId, String targetGroupId) {
        int initialLoadOrder = 1;
        List<String> tables = getRootConfigChannelTableNames();
        List<TriggerRouter> triggers = new ArrayList<TriggerRouter>(tables.size());
        for (int j = 0; j < tables.size(); j++) {
            String tableName = tables.get(j);
            boolean syncChanges = !TableConstants.getNodeTablesAsSet(tablePrefix).contains(tableName);
            TriggerRouter trigger = buildConfigTrigger(tableName, syncChanges, sourceGroupId, targetGroupId);
            trigger.setInitialLoadOrder(initialLoadOrder++);
            String initialLoadSelect = rootConfigChannelInitialLoadSelect.get(tableName);
            trigger.getRouter().setInitialLoadSelect(initialLoadSelect);
            triggers.add(trigger);
        }
        return triggers;
    }

    protected TriggerRouter buildConfigTrigger(String tableName, boolean syncChanges, String sourceGroupId,
            String targetGroupId) {
        boolean autoSyncConfig = parameterService.is(ParameterConstants.AUTO_SYNC_CONFIGURATION);
        TriggerRouter triggerRouter = new TriggerRouter();
        Trigger trigger = triggerRouter.getTrigger();
        trigger.setTriggerId(Math.abs(tableName.hashCode() + targetGroupId.hashCode()));
        trigger.setSyncOnDelete(syncChanges && autoSyncConfig);
        trigger.setSyncOnInsert(syncChanges && autoSyncConfig);
        trigger.setSyncOnUpdate(syncChanges && autoSyncConfig);
        trigger.setSyncOnIncomingBatch(true);
        trigger.setSourceTableName(tableName);
        trigger.setChannelId(Constants.CHANNEL_CONFIG);

        Router router = triggerRouter.getRouter();
        router.setSourceNodeGroupId(sourceGroupId);
        router.setTargetNodeGroupId(targetGroupId);
        router.setInitialLoadSelect(rootConfigChannelInitialLoadSelect.get(tableName));

        // little trick to force the rebuild of sym triggers every time
        // there is a new version of symmetricds
        trigger.setLastUpdateTime(new Date(Version.version().hashCode()));
        router.setLastUpdateTime(trigger.getLastUpdateTime());
        triggerRouter.setLastUpdateTime(trigger.getLastUpdateTime());
        return triggerRouter;
    }
    
    private String getTriggerRouterSqlPrefix() {
        return getSql("selectTriggerRouterPrefixSql");
    }

    /**
     * Create triggers on SymmetricDS tables so changes to configuration can be synchronized.
     */
    protected List<TriggerRouter> getConfigurationTriggers(String sourceNodeGroupId) {
        List<TriggerRouter> triggers = new ArrayList<TriggerRouter>();
        List<NodeGroupLink> links = configurationService.getGroupLinksFor(sourceNodeGroupId);
        for (NodeGroupLink nodeGroupLink : links) {
            if (nodeGroupLink.getDataEventAction().equals(DataEventAction.WAIT_FOR_PULL)) {
                triggers.addAll(getConfigurationTriggers(nodeGroupLink.getSourceGroupId(), nodeGroupLink
                        .getTargetGroupId()));
            } else if (nodeGroupLink.getDataEventAction().equals(DataEventAction.PUSH)) {
                triggers.add(buildConfigTrigger(TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE),
                        false, nodeGroupLink.getSourceGroupId(), nodeGroupLink.getTargetGroupId()));             
                    log.debug("TriggerHistCreating", TableConstants.getTableName(tablePrefix, TableConstants.SYM_NODE));

                } else {
                    log.warn("TriggerConfigurationCreatingFailed", sourceNodeGroupId, nodeGroupLink.getDataEventAction());
                }
        }
        return triggers;
    }

    @SuppressWarnings("unchecked")
    public TriggerRouter getTriggerFor(String table, String sourceNodeGroupId) {
        List<TriggerRouter> configs = (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSqlPrefix() + getSql("selectTriggerSql"),
                new Object[] { table, sourceNodeGroupId }, new TriggerRouterMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            List<TriggerRouter> triggers = getActiveTriggersForSourceNodeGroup(sourceNodeGroupId);
            for (TriggerRouter trigger : triggers) {
                if (trigger.getTrigger().getSourceTableName().equalsIgnoreCase(table)) {
                    return trigger;
                }
            }
        }
        return null;
    }

    protected void mergeInConfigurationTriggers(String sourceNodeGroupId, List<TriggerRouter> configuredInDatabase) {
        List<TriggerRouter> virtualConfigTriggers = getConfigurationTriggers(sourceNodeGroupId);
        for (TriggerRouter trigger : virtualConfigTriggers) {
            if (trigger.getRouter().getSourceNodeGroupId().equalsIgnoreCase(sourceNodeGroupId)
                    && !doesTriggerExistInList(configuredInDatabase, trigger)) {
                configuredInDatabase.add(trigger);
            }
        }
    }

    protected boolean doesTriggerExistInList(List<TriggerRouter> triggers, TriggerRouter trigger) {
        for (TriggerRouter checkMe : triggers) {
            if (checkMe.getTrigger().isSame(trigger.getTrigger())) {
                return true;
            }
        }
        return false;
    }

    public Map<Integer, List<TriggerRouter>> getActiveTriggersForSourceNodeGroup(String sourceNodeGroupId,
            boolean refreshCache) {
        if (triggerCache == null || refreshCache) {
            synchronized (this) {
                triggerCache = new HashMap<Integer, List<TriggerRouter>>();
                List<TriggerRouter> triggers = getActiveTriggersForSourceNodeGroup(sourceNodeGroupId);
                for (TriggerRouter trigger : triggers) {
                    int triggerId = trigger.getTrigger().getTriggerId();
                    List<TriggerRouter> list = triggerCache.get(triggerId);
                    if (list == null) {
                        list = new ArrayList<TriggerRouter>();
                        triggerCache.put(triggerId, list);
                    }
                    list.add(trigger);
                }
            }
        }
        return triggerCache;
    }

    @SuppressWarnings("unchecked")
    public List<TriggerRouter> getActiveTriggersForSourceNodeGroup(String sourceNodeGroupId) {
        List<TriggerRouter> triggers = (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSqlPrefix() + 
                getSql("activeTriggersForSourceNodeGroupSql"), new Object[] { sourceNodeGroupId },
                new TriggerRouterMapper());
        mergeInConfigurationTriggers(sourceNodeGroupId, triggers);
        return triggers;
    }

    @SuppressWarnings("unchecked")
    public List<TriggerRouter> getActiveTriggersForReload(String sourceNodeGroupId, String targetNodeGroupId) {
        return (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSqlPrefix() + getSql("activeTriggersForReloadSql"), new Object[] {
                sourceNodeGroupId, targetNodeGroupId, Constants.CHANNEL_CONFIG }, new TriggerRouterMapper());
    }

    @SuppressWarnings("unchecked")
    public List<TriggerRouter> getInactiveTriggersForSourceNodeGroup(String sourceNodeGroupId) {
        return (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSqlPrefix() + getSql("inactiveTriggersForSourceNodeGroupSql"),
                new Object[] { sourceNodeGroupId }, new TriggerRouterMapper());
    }

    @SuppressWarnings("unchecked")
    public TriggerRouter getTriggerForTarget(String table, String sourceNodeGroupId, String targetNodeGroupId,
            String channel) {
        List<TriggerRouter> configs = (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSqlPrefix() + getSql("selectTriggerTargetSql"),
                new Object[] { table, targetNodeGroupId, channel, sourceNodeGroupId }, new TriggerRouterMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerById(int triggerId) {
        List<TriggerRouter> triggers = (List<TriggerRouter>) jdbcTemplate.query(getTriggerRouterSqlPrefix() + getSql("selectTriggerByIdSql"),
                new Object[] { triggerId }, new TriggerRouterMapper());
        if (triggers.size() > 0) {
            return triggers.get(0).getTrigger();
        } else {
            return null;
        }
    }

    public Map<String, List<TriggerRouter>> getTriggersByChannelFor(String nodeGroupId) {
        final Map<String, List<TriggerRouter>> retMap = new HashMap<String, List<TriggerRouter>>();
        jdbcTemplate.query(getTriggerRouterSqlPrefix() + getSql("selectGroupTriggersSql"), new Object[] { nodeGroupId }, new TriggerRouterMapper() {
            public Object mapRow(java.sql.ResultSet rs, int arg1) throws java.sql.SQLException {
                TriggerRouter config = (TriggerRouter) super.mapRow(rs, arg1);
                List<TriggerRouter> list = retMap.get(config.getTrigger().getChannelId());
                if (list == null) {
                    list = new ArrayList<TriggerRouter>();
                    retMap.put(config.getTrigger().getChannelId(), list);
                }
                list.add(config);
                return config;
            };
        });
        return retMap;
    }

    public void insert(TriggerHistory newHistRecord) {
        jdbcTemplate.update(getSql("insertTriggerHistorySql"), new Object[] { newHistRecord.getTriggerId(),
                newHistRecord.getSourceTableName(), newHistRecord.getTableHash(), newHistRecord.getCreateTime(),
                newHistRecord.getColumnNames(), newHistRecord.getPkColumnNames(),
                newHistRecord.getLastTriggerBuildReason().getCode(), newHistRecord.getNameForDeleteTrigger(),
                newHistRecord.getNameForInsertTrigger(), newHistRecord.getNameForUpdateTrigger(),
                newHistRecord.getSourceSchemaName(), newHistRecord.getSourceCatalogName(),
                newHistRecord.getTriggerRowHash() }, new int[] { Types.INTEGER, Types.VARCHAR, Types.BIGINT,
                Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.CHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.BIGINT });
    }

    public void saveTriggerRouter(TriggerRouter triggerRouter) {
        saveTrigger(triggerRouter.getTrigger());
        saveRouter(triggerRouter.getRouter());
        triggerRouter.setLastUpdateTime(new Date());
        if (0 == jdbcTemplate.update(getSql("updateTriggerRouterSql"), new Object[] {
                triggerRouter.getInitialLoadOrder(), triggerRouter.getLastUpdateBy(), triggerRouter.getLastUpdateTime(),
                triggerRouter.getTrigger().getTriggerId(), triggerRouter.getRouter().getRouterId() }, new int[] {
                Types.INTEGER, Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER, Types.INTEGER })) {
            triggerRouter.setCreateTime(triggerRouter.getLastUpdateTime());
            jdbcTemplate.update(getSql("insertTriggerRouterSql"), new Object[] { triggerRouter.getInitialLoadOrder(),
                    triggerRouter.getCreateTime(), triggerRouter.getLastUpdateBy(), triggerRouter.getLastUpdateTime(), triggerRouter.getTrigger().getTriggerId(),
                    triggerRouter.getRouter().getRouterId() }, new int[] { Types.INTEGER, Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP,
                     Types.INTEGER, Types.INTEGER });
        }
    }

    public void saveRouter(Router router) {
        router.setLastUpdateTime(new Date());
        if (0 == jdbcTemplate.update(getSql("updateRouterSql"), new Object[] { router.getTargetCatalogName(),
                router.getTargetSchemaName(), router.getTargetTableName(), router.getSourceNodeGroupId(),
                router.getTargetNodeGroupId(), router.getRouterName(), router.getRouterExpression(),
                router.getInitialLoadSelect(), router.getLastUpdateBy(), router.getLastUpdateTime(),
                router.getRouterId() }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, 
                Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER })) {
            router.setCreateTime(router.getLastUpdateTime());
            jdbcTemplate.update(getSql("insertRouterSql"), new Object[] { router.getTargetCatalogName(),
                    router.getTargetSchemaName(), router.getTargetTableName(), router.getSourceNodeGroupId(),
                    router.getTargetNodeGroupId(), router.getRouterName(), router.getRouterExpression(),
                    router.getInitialLoadSelect(), router.getCreateTime(), router.getLastUpdateBy(), router.getLastUpdateTime(),
                    router.getRouterId() }, new int[] { Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                    Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER });
        }
    }

    public void saveTrigger(Trigger trigger) {
        trigger.setLastUpdateTime(new Date());
        if (0 == jdbcTemplate.update(getSql("updateTriggerSql"), new Object[] { trigger.getSourceCatalogName(),
                trigger.getSourceSchemaName(), trigger.getSourceTableName(), trigger.getChannelId(),
                trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0, trigger.isSyncOnDelete() ? 1 : 0,
                trigger.isSyncOnIncomingBatch() ? 1 : 0, trigger.getNameForUpdateTrigger(),
                trigger.getNameForInsertTrigger(), trigger.getNameForDeleteTrigger(),
                trigger.getSyncOnUpdateCondition(), trigger.getSyncOnInsertCondition(),
                trigger.getSyncOnDeleteCondition(), trigger.getTxIdExpression(), trigger.getExcludedColumnNames(),
                trigger.getInactiveTime(), trigger.getLastUpdateBy(), trigger.getLastUpdateTime(), trigger.getTriggerId() }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT,
                Types.SMALLINT, Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP,
                Types.VARCHAR, Types.TIMESTAMP, Types.INTEGER })) {
            trigger.setCreateTime(trigger.getLastUpdateTime());
            jdbcTemplate.update(getSql("insertTriggerSql"), new Object[] { trigger.getSourceCatalogName(),
                    trigger.getSourceSchemaName(), trigger.getSourceTableName(), trigger.getChannelId(),
                    trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0,
                    trigger.isSyncOnDelete() ? 1 : 0, trigger.isSyncOnIncomingBatch() ? 1 : 0,
                    trigger.getNameForUpdateTrigger(), trigger.getNameForInsertTrigger(),
                    trigger.getNameForDeleteTrigger(), trigger.getSyncOnUpdateCondition(),
                    trigger.getSyncOnInsertCondition(), trigger.getSyncOnDeleteCondition(),
                    trigger.getTxIdExpression(), trigger.getExcludedColumnNames(), trigger.getCreateTime(), null,
                    trigger.getLastUpdateBy(), trigger.getLastUpdateTime(), trigger.getTriggerId() }, new int[] { Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                    Types.SMALLINT, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                    Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.TIMESTAMP, Types.TIMESTAMP, Types.VARCHAR,
                    Types.TIMESTAMP, Types.INTEGER });
        }
    }

    public Map<Long, TriggerHistory> getHistoryRecords() {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        jdbcTemplate.query(getSql("allTriggerHistSql"), new TriggerHistoryMapper(retMap));
        return retMap;
    }

    public TriggerHistory getTriggerHistoryForSourceTable(String sourceTableName) {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        jdbcTemplate.query(String.format("%s%s", getSql("allTriggerHistSql"),
                getSql("triggerHistBySourceTableWhereSql")), new Object[] { sourceTableName },
                new int[] { Types.VARCHAR }, new TriggerHistoryMapper(retMap));
        if (retMap.size() > 0) {
            return retMap.values().iterator().next();
        } else {
            return null;
        }
    }

    public TriggerHistory getHistoryRecordFor(int histId) {
        TriggerHistory history = historyMap.get(histId);
        if (history == null && histId >= 0) {
            try {
                history = (TriggerHistory) jdbcTemplate.queryForObject(getSql("triggerHistSql"),
                        new Object[] { histId }, new TriggerHistoryMapper());
                historyMap.put(histId, history);
            } catch (EmptyResultDataAccessException ex) {
            }
        }
        return history;
    }

    public TriggerHistory getLatestHistoryRecordFor(int triggerId) {
        try {
            return (TriggerHistory) jdbcTemplate.queryForObject(getSql("latestTriggerHistSql"),
                    new Object[] { triggerId }, new TriggerHistoryMapper());
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    public void syncTriggers() {
        syncTriggers(null, false);
    }

    synchronized public void syncTriggers(StringBuilder sqlBuffer, boolean gen_always) {
        if (clusterService.lock(LockActionConstants.SYNCTRIGGERS)) {
            try {
                log.info("TriggersSynchronizing");
                // make sure channels are read from the database
                configurationService.reloadChannels();
                removeInactiveTriggers(sqlBuffer);
                updateOrCreateDatabaseTriggers(sqlBuffer, gen_always);
            } finally {
                clusterService.unlock(LockActionConstants.SYNCTRIGGERS);
                log.info("TriggersSynchronized");
            }
        } else {
            log.info("TriggersSynchronizingFailedLock");
        }
    }

    private void removeInactiveTriggers(StringBuilder sqlBuffer) {
        List<Trigger> triggers = new TriggerSelector(getInactiveTriggersForSourceNodeGroup(parameterService
                .getString(ParameterConstants.NODE_GROUP_ID))).select();
        for (Trigger trigger : triggers) {
            TriggerHistory history = getLatestHistoryRecordFor(trigger.getTriggerId());
            if (history != null) {
                log.info("TriggersRemoving", history.getSourceTableName());
                dbDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(), history.getSourceSchemaName(),
                        history.getNameForInsertTrigger(), trigger.getSourceTableName(), history);
                dbDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(), history.getSourceSchemaName(),
                        history.getNameForDeleteTrigger(), trigger.getSourceTableName(), history);
                dbDialect.removeTrigger(sqlBuffer, history.getSourceCatalogName(), history.getSourceSchemaName(),
                        history.getNameForUpdateTrigger(), trigger.getSourceTableName(), history);

                if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                    if (this.triggerCreationListeners != null) {
                        for (ITriggerCreationListener l : this.triggerCreationListeners) {
                            l.triggerInactivated(trigger, history);
                        }
                    }
                }

                boolean triggerExists = dbDialect.doesTriggerExist(history.getSourceCatalogName(), history
                        .getSourceSchemaName(), history.getSourceTableName(), history.getNameForInsertTrigger());
                triggerExists |= dbDialect.doesTriggerExist(history.getSourceCatalogName(), history
                        .getSourceSchemaName(), history.getSourceTableName(), history.getNameForUpdateTrigger());
                triggerExists |= dbDialect.doesTriggerExist(history.getSourceCatalogName(), history
                        .getSourceSchemaName(), history.getSourceTableName(), history.getNameForDeleteTrigger());
                if (triggerExists) {
                    log.warn("TriggersRemovingFailed", history.getTriggerId(), history.getTriggerHistoryId());
                } else {
                    inactivateTriggerHistory(history);
                }

            } else {
                log.info("TriggersRemovingSkipped");
            }
        }
    }

    private List<TriggerRouter> toList(Collection<List<TriggerRouter>> source) {
        ArrayList<TriggerRouter> list = new ArrayList<TriggerRouter>();
        for (List<TriggerRouter> triggerRouters : source) {
            for (TriggerRouter triggerRouter : triggerRouters) {
                list.add(triggerRouter);
            }
        }
        return list;
    }

    private void updateOrCreateDatabaseTriggers(StringBuilder sqlBuffer, boolean gen_always) {
        List<Trigger> triggers = new TriggerSelector(toList(getActiveTriggersForSourceNodeGroup(
                parameterService.getString(ParameterConstants.NODE_GROUP_ID), true).values())).select();

        for (Trigger trigger : triggers) {

            String schemaPlusTriggerName = (trigger.getSourceSchemaName() != null ? trigger.getSourceSchemaName() + "."
                    : "")
                    + trigger.getSourceTableName();

            try {

                TriggerReBuildReason reason = TriggerReBuildReason.NEW_TRIGGERS;

                Table table = dbDialect.getMetaDataFor(trigger.getSourceCatalogName(), trigger.getSourceSchemaName(),
                        trigger.getSourceTableName(), false);

                if (table != null) {
                    TriggerHistory latestHistoryBeforeRebuild = getLatestHistoryRecordFor(trigger.getTriggerId());

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
                    } else if (gen_always) {
                        reason = TriggerReBuildReason.FORCED;
                        forceRebuildOfTriggers = true;
                    }

                    TriggerHistory newestHistory = rebuildTriggerIfNecessary(sqlBuffer, forceRebuildOfTriggers,
                            trigger, DataEventType.INSERT, reason, latestHistoryBeforeRebuild, null, trigger
                                    .isSyncOnInsert(), table);

                    newestHistory = rebuildTriggerIfNecessary(sqlBuffer, forceRebuildOfTriggers, trigger,
                            DataEventType.UPDATE, reason, latestHistoryBeforeRebuild, newestHistory, trigger
                                    .isSyncOnUpdate(), table);

                    newestHistory = rebuildTriggerIfNecessary(sqlBuffer, forceRebuildOfTriggers, trigger,
                            DataEventType.DELETE, reason, latestHistoryBeforeRebuild, newestHistory, trigger
                                    .isSyncOnDelete(), table);

                    if (latestHistoryBeforeRebuild != null && newestHistory != null) {
                        inactivateTriggerHistory(latestHistoryBeforeRebuild);
                    }

                    if (newestHistory != null) {
                        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
                            if (this.triggerCreationListeners != null) {
                                for (ITriggerCreationListener l : this.triggerCreationListeners) {
                                    l.triggerCreated(trigger, newestHistory);
                                }
                            }
                        }
                    }

                } else {
                    log.error("TriggerTableMissing", schemaPlusTriggerName);

                    if (this.triggerCreationListeners != null) {
                        for (ITriggerCreationListener l : this.triggerCreationListeners) {
                            l.tableDoesNotExist(trigger);
                        }
                    }
                }
            } catch (Exception ex) {
                log.error("TriggerSynchronizingFailed", schemaPlusTriggerName, ex);
                if (this.triggerCreationListeners != null) {
                    for (ITriggerCreationListener l : this.triggerCreationListeners) {
                        l.triggerFailed(trigger, ex);
                    }
                }
            }

        }
    }

    private TriggerHistory rebuildTriggerIfNecessary(StringBuilder sqlBuffer, boolean forceRebuild, Trigger trigger,
            DataEventType dmlType, TriggerReBuildReason reason, TriggerHistory oldhist, TriggerHistory hist,
            boolean triggerIsActive, Table table) {

        boolean triggerExists = false;

        TriggerHistory newTriggerHist = new TriggerHistory(table, trigger, reason);
        int maxTriggerNameLength = dbDialect.getMaxTriggerNameLength();
        String triggerPrefix = parameterService.getString(ParameterConstants.RUNTIME_CONFIG_TABLE_PREFIX);
        newTriggerHist.setNameForInsertTrigger(dbDialect.getTriggerName(DataEventType.INSERT, triggerPrefix,
                maxTriggerNameLength, trigger, hist).toUpperCase());
        newTriggerHist.setNameForUpdateTrigger(dbDialect.getTriggerName(DataEventType.UPDATE, triggerPrefix,
                maxTriggerNameLength, trigger, hist).toUpperCase());
        newTriggerHist.setNameForDeleteTrigger(dbDialect.getTriggerName(DataEventType.DELETE, triggerPrefix,
                maxTriggerNameLength, trigger, hist).toUpperCase());

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
            // defined in the trigger row data does not exist as well.
            oldTriggerName = newTriggerHist.getTriggerNameForDmlType(dmlType);
            oldSourceSchema = trigger.getSourceSchemaName();
            oldCatalogName = trigger.getSourceCatalogName();
            triggerExists = dbDialect.doesTriggerExist(oldCatalogName, oldSourceSchema, trigger.getSourceTableName(),
                    oldTriggerName);
        }

        if (!triggerExists && forceRebuild) {
            reason = TriggerReBuildReason.TRIGGERS_MISSING;
        }

        if ((forceRebuild || !triggerIsActive) && triggerExists) {
            dbDialect.removeTrigger(sqlBuffer, oldCatalogName, oldSourceSchema, oldTriggerName, trigger
                    .getSourceTableName(), oldhist);
            triggerExists = false;
        }

        boolean isDeadTrigger = !trigger.isSyncOnInsert() && !trigger.isSyncOnUpdate() && !trigger.isSyncOnDelete();

        if (hist == null && (oldhist == null || (!triggerExists && triggerIsActive) || (isDeadTrigger && forceRebuild))) {
            insert(newTriggerHist);
            hist = getLatestHistoryRecordFor(trigger.getTriggerId());
        }

        if (!triggerExists && triggerIsActive) {
            dbDialect.initTrigger(sqlBuffer, dmlType, trigger, hist, tablePrefix, table);
        }

        return hist;
    }

    class NodeGroupLinkMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            NodeGroupLink node_groupTarget = new NodeGroupLink();
            node_groupTarget.setSourceGroupId(rs.getString(1));
            node_groupTarget.setTargetGroupId(rs.getString(2));
            node_groupTarget.setDataEventAction(DataEventAction.fromCode(rs.getString(3)));
            return node_groupTarget;
        }
    }

    class TriggerHistoryMapper implements RowMapper {
        Map<Long, TriggerHistory> retMap = null;

        TriggerHistoryMapper() {
        }

        TriggerHistoryMapper(Map<Long, TriggerHistory> map) {
            this.retMap = map;
        }

        public Object mapRow(ResultSet rs, int i) throws SQLException {
            TriggerHistory hist = new TriggerHistory();
            hist.setTriggerHistoryId(rs.getInt(1));
            hist.setTriggerId(rs.getInt(2));
            hist.setSourceTableName(rs.getString(3));
            hist.setTableHash(rs.getInt(4));
            hist.setCreateTime(rs.getTimestamp(5));
            hist.setPkColumnNames(rs.getString(6));
            hist.setColumnNames(rs.getString(7));
            hist.setLastTriggerBuildReason(TriggerReBuildReason.fromCode(rs.getString(8)));
            hist.setNameForDeleteTrigger(rs.getString(9));
            hist.setNameForInsertTrigger(rs.getString(10));
            hist.setNameForUpdateTrigger(rs.getString(11));
            hist.setSourceSchemaName(rs.getString(12));
            hist.setSourceCatalogName(rs.getString(13));
            hist.setTriggerRowHash(rs.getLong(14));
            if (this.retMap != null) {
                this.retMap.put((long) hist.getTriggerHistoryId(), hist);
            }
            return hist;
        }
    }

    class TriggerRouterMapper implements RowMapper {
        public Object mapRow(java.sql.ResultSet rs, int arg1) throws java.sql.SQLException {
            TriggerRouter trig = new TriggerRouter();
            trig.getTrigger().setTriggerId(rs.getInt("trigger_id"));
            trig.getTrigger().setChannelId(rs.getString("channel_id"));
            trig.getTrigger().setSourceTableName(rs.getString("source_table_name"));
            trig.getTrigger().setInactiveTime(rs.getTimestamp("inactive_time"));
            trig.getTrigger().setSyncOnInsert(rs.getBoolean("sync_on_insert"));
            trig.getTrigger().setSyncOnUpdate(rs.getBoolean("sync_on_update"));
            trig.getTrigger().setSyncOnDelete(rs.getBoolean("sync_on_delete"));
            trig.getTrigger().setSyncOnIncomingBatch(rs.getBoolean("sync_on_incoming_batch"));
            trig.getTrigger().setSyncColumnLevel(rs.getBoolean("sync_column_level"));
            trig.getTrigger().setNameForDeleteTrigger(rs.getString("name_for_delete_trigger"));
            trig.getTrigger().setNameForInsertTrigger(rs.getString("name_for_insert_trigger"));
            trig.getTrigger().setNameForUpdateTrigger(rs.getString("name_for_update_trigger"));
            String schema = rs.getString("source_schema_name");
            trig.getTrigger().setSourceSchemaName(schema);
            String catalog = rs.getString("source_catalog_name");
            trig.getTrigger().setSourceCatalogName(catalog);
            String condition = rs.getString("sync_on_insert_condition");
            if (!StringUtils.isBlank(condition)) {
                trig.getTrigger().setSyncOnInsertCondition(condition);
            }
            condition = rs.getString("sync_on_update_condition");
            if (!StringUtils.isBlank(condition)) {
                trig.getTrigger().setSyncOnUpdateCondition(condition);
            }

            condition = rs.getString("sync_on_delete_condition");
            if (!StringUtils.isBlank(condition)) {
                trig.getTrigger().setSyncOnDeleteCondition(condition);
            }
            trig.getTrigger().setTxIdExpression(rs.getString("tx_id_expression"));
            trig.getTrigger().setCreateTime(rs.getTimestamp("t_create_time"));
            trig.getTrigger().setLastUpdateTime(rs.getTimestamp("t_last_update_time"));
            trig.getTrigger().setLastUpdateBy(rs.getString("t_last_update_by"));

            trig.getRouter().setTargetCatalogName(rs.getString("target_catalog_name"));
            trig.getRouter().setSourceNodeGroupId(rs.getString("source_node_group_id"));
            trig.getRouter().setTargetSchemaName(rs.getString("target_schema_name"));
            trig.getRouter().setTargetTableName(rs.getString("target_table_name"));
            trig.getRouter().setTargetNodeGroupId(rs.getString("target_node_group_id"));
            trig.getTrigger().setExcludedColumnNames(rs.getString("excluded_column_names"));
            trig.getRouter().setInitialLoadSelect(rs.getString("initial_load_select"));

            condition = rs.getString("router_expression");
            if (!StringUtils.isBlank(condition)) {
                trig.getRouter().setRouterExpression(condition);
            }
            trig.getRouter().setRouterName(rs.getString("router_name"));
            trig.getRouter().setRouterId(rs.getInt("router_id"));
            trig.getRouter().setCreateTime(rs.getTimestamp("r_create_time"));
            trig.getRouter().setLastUpdateTime(rs.getTimestamp("r_last_update_time"));
            trig.getRouter().setLastUpdateBy(rs.getString("r_last_update_by"));

            trig.setCreateTime(rs.getTimestamp("create_time"));
            trig.setLastUpdateTime(rs.getTimestamp("last_update_time"));
            trig.setLastUpdateBy(rs.getString("last_update_by"));
            trig.setInitialLoadOrder(rs.getInt("initial_load_order"));

            return trig;
        }
    }

    public void setRootConfigChannelTableNames(List<String> configChannelTableNames) {
        this.rootConfigChannelTableNames = configChannelTableNames;
    }

    public void setRootConfigChannelInitialLoadSelect(Map<String, String> rootConfigChannelInitialLoadSelect) {
        this.rootConfigChannelInitialLoadSelect = rootConfigChannelInitialLoadSelect;
    }

    public void setClusterService(IClusterService clusterService) {
        this.clusterService = clusterService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

    public void setTriggerCreationListeners(List<ITriggerCreationListener> autoTriggerCreationListeners) {
        if (triggerCreationListeners != null) {
            for (ITriggerCreationListener l : triggerCreationListeners) {
                addTriggerCreationListeners(l);
            }
        }
    }

    public void addTriggerCreationListeners(ITriggerCreationListener l) {
        if (this.triggerCreationListeners == null) {
            this.triggerCreationListeners = new ArrayList<ITriggerCreationListener>();
        }
        this.triggerCreationListeners.add(l);
    }

    public Map<Trigger, Exception> getFailedTriggers() {
        return this.failureListener.getFailures();
    }
}
