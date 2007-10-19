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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.WeakHashMap;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.DataEventAction;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupLink;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerReBuildReason;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.RowMapper;

public class ConfigurationService extends AbstractService implements
        IConfigurationService {

    final static Log logger = LogFactory.getLog(ConfigurationService.class);

    private String triggerHistSql;

    private String latestTriggerHistSql;

    private String allTriggerHistSql;

    private String insertChannelSql;

    private String groupsLinksForSql;

    private String insertTriggerSql;

    private String selectTriggerSql;
    
    private String selectTriggerByIdSql;

    private String selectTriggerTargetSql;

    private String selectGroupTriggersSql;

    private String selectChannelsSql;

    private String selectDataEventActionsByIdSql;

    private String insertTriggerHistorySql;

    private String inactivateTriggerHistorySql;

    private String activeTriggersForSourceNodeGroupSql;
    
    private String activeTriggersForReloadSql;

    private String inactiveTriggersForSourceNodeGroupSql;

    private List<String> rootConfigChannelTableNames;

    private List<String> nodeConfigChannelTableNames;

    private IDbDialect dbDialect;
    
    private WeakHashMap<Integer, TriggerHistory> historyMap = new WeakHashMap<Integer, TriggerHistory>();

    public void initSystemChannels() {
        try {
            jdbcTemplate.update(insertChannelSql, new Object[] { Constants.CHANNEL_CONFIG, 0, 100 });
        } catch (DataIntegrityViolationException ex) {
            logger.debug("Channel " + Constants.CHANNEL_CONFIG + " already created.");
        }
        try {
            jdbcTemplate.update(insertChannelSql, new Object[] { Constants.CHANNEL_RELOAD, 1, 100000 });
        } catch (DataIntegrityViolationException ex) {
            logger.debug("Channel " + Constants.CHANNEL_RELOAD + " already created.");
        }
    }

    public void inactivateTriggerHistory(TriggerHistory history) {
        jdbcTemplate.update(inactivateTriggerHistorySql, new Object[] { history
                .getTriggerHistoryId() });
    }

    @SuppressWarnings("unchecked")
    public List<NodeGroupLink> getGroupLinksFor(String nodeGroupId) {
        return jdbcTemplate.query(groupsLinksForSql,
                new Object[] { nodeGroupId }, new DomainTargetRowMapper());
    }

    public List<String> getRootConfigChannelTableNames() {
        return rootConfigChannelTableNames;
    }

    public void initTriggerRowsForConfigChannel() {
        List<String> tableNames = null;
        if (StringUtils.isEmpty(runtimeConfiguration.getRegistrationUrl())) {
            tableNames = getRootConfigChannelTableNames();
        } else {
            tableNames = getNodeConfigChannelTableNames();
        }
        initSystemChannels();
        String groupId = runtimeConfiguration.getNodeGroupId();
        List<NodeGroupLink> targets = getGroupLinksFor(groupId);
        if (targets != null && targets.size() > 0) {
            for (NodeGroupLink target : targets) {
                int initialLoadOrder = 1;
                for (String tableName : tableNames) {
                    Trigger trigger = getTriggerForTarget(tableName, groupId, target.getTargetGroupId(),
                            Constants.CHANNEL_CONFIG);
                    if (trigger == null) {
                        jdbcTemplate.update(insertTriggerSql, new Object[] { tableName, groupId,
                                target.getTargetGroupId(), Constants.CHANNEL_CONFIG, initialLoadOrder++ });
                    }
                }
            }
        } else {
            logger.error("Could not find any targets for your group id of "
                    + runtimeConfiguration.getNodeGroupId()
                    + ".  Please validate your node group id against the setup in the database.");
        }
    }

    @SuppressWarnings("unchecked")
    public List<NodeChannel> getChannelsFor(boolean failOnError) {
        try {
            return jdbcTemplate.query(selectChannelsSql,
                    new Object[] { }, new RowMapper() {
                        public Object mapRow(java.sql.ResultSet rs, int arg1)
                                throws java.sql.SQLException {
                            NodeChannel channel = new NodeChannel();
                            channel.setId(rs.getString(1));
                            channel.setNodeId(rs.getString(2));
                            channel.setIgnored(isSet(rs
                                    .getObject(3)));
                            channel.setSuspended(isSet(rs
                                    .getObject(4)));
                            channel.setProcessingOrder(rs
                                    .getInt(5));
                            channel
                                    .setMaxBatchSize(rs
                                            .getInt(6));
                            channel.setEnabled(rs.getBoolean(7));
                            return channel;
                        };
                    });
        } catch (RuntimeException ex) {
            if (failOnError) {
                throw ex;
            } else {
                return new ArrayList<NodeChannel>(0);
            }
        }
    }

    private boolean isSet(Object value) {
        if (value != null && value.toString().equals("1")) {
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public Map<String, DataEventAction> getDataEventActionsByGroupId(
            String nodeGroupId) {
        Map<String, String> results = (Map<String, String>) jdbcTemplate
                .queryForMap(selectDataEventActionsByIdSql,
                        new Object[] { nodeGroupId });
        Map<String, DataEventAction> retMap = new HashMap<String, DataEventAction>();
        for (String key : results.keySet()) {
            retMap.put(key, DataEventAction.fromCode(results.get(key)));
        }
        return retMap;
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerFor(String table, String sourceNodeGroupId) {
        List<Trigger> configs = (List<Trigger>) jdbcTemplate.query(
                selectTriggerSql, new Object[] { table, sourceNodeGroupId },
                new TriggerMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<Trigger> getActiveTriggersForSourceNodeGroup(
            String sourceNodeGroupId) {
        return (List<Trigger>) jdbcTemplate.query(
                activeTriggersForSourceNodeGroupSql,
                new Object[] { sourceNodeGroupId }, new TriggerMapper());
    }

    @SuppressWarnings("unchecked")
    public List<Trigger> getActiveTriggersForReload(String sourceNodeGroupId, String targetNodeGroupId) {
        return (List<Trigger>) jdbcTemplate.query(activeTriggersForReloadSql, new Object[] {
                sourceNodeGroupId, targetNodeGroupId, Constants.CHANNEL_CONFIG }, new TriggerMapper());
    }

    @SuppressWarnings("unchecked")
    public List<Trigger> getInactiveTriggersForSourceNodeGroup(
            String sourceNodeGroupId) {
        return (List<Trigger>) jdbcTemplate.query(
                inactiveTriggersForSourceNodeGroupSql,
                new Object[] { sourceNodeGroupId }, new TriggerMapper());
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerForTarget(String table, String sourceNodeGroupId,
            String targetNodeGroupId, String channel) {
        List<Trigger> configs = (List<Trigger>) jdbcTemplate.query(
                selectTriggerTargetSql, new Object[] { table,
                        targetNodeGroupId, channel, sourceNodeGroupId },
                new TriggerMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerById(int triggerId) {
        List<Trigger> triggers = (List<Trigger>) jdbcTemplate.query(selectTriggerByIdSql,
                new Object[] { triggerId }, new TriggerMapper());
        if (triggers.size() > 0) {
            return triggers.get(0);
        } else {
            return null;
        }
    }

    public Map<String, List<Trigger>> getTriggersByChannelFor(String nodeGroupId) {
        final Map<String, List<Trigger>> retMap = new HashMap<String, List<Trigger>>();
        jdbcTemplate.query(selectGroupTriggersSql,
                new Object[] { nodeGroupId }, new TriggerMapper() {
                    public Object mapRow(java.sql.ResultSet rs, int arg1)
                            throws java.sql.SQLException {
                        Trigger config = (Trigger) super.mapRow(rs, arg1);
                        List<Trigger> list = retMap.get(config.getChannelId());
                        if (list == null) {
                            list = new ArrayList<Trigger>();
                            retMap.put(config.getChannelId(), list);
                        }
                        list.add(config);
                        return config;
                    };
                });
        return retMap;
    }

    public void insert(TriggerHistory newHistRecord) {
        jdbcTemplate.update(insertTriggerHistorySql, new Object[] {
                newHistRecord.getTriggerId(),
                newHistRecord.getSourceTableName(),
                newHistRecord.getTableHash(), newHistRecord.getCreateTime(),
                newHistRecord.getColumnNames(),
                newHistRecord.getPkColumnNames(),
                newHistRecord.getLastTriggerBuildReason().getCode(),
                newHistRecord.getNameForDeleteTrigger(),
                newHistRecord.getNameForInsertTrigger(),
                newHistRecord.getNameForUpdateTrigger(),
                newHistRecord.getSourceSchemaName() });
    }

    public Map<Long, TriggerHistory> getHistoryRecords() {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        jdbcTemplate.query(this.allTriggerHistSql, new TriggerHistoryMapper(
                retMap));
        return retMap;
    }

    public TriggerHistory getHistoryRecordFor(int auditId) {
        TriggerHistory history = historyMap.get(auditId);
        if (history == null) {
            try {
                history = (TriggerHistory) jdbcTemplate.queryForObject(this.triggerHistSql,
                        new Object[] { auditId }, new TriggerHistoryMapper());
                historyMap.put(auditId, history);
            } catch (EmptyResultDataAccessException ex) {
            }
        }
        return history;
    }

    public TriggerHistory getLatestHistoryRecordFor(int triggerId) {
        try {
            return (TriggerHistory) jdbcTemplate.queryForObject(
                    this.latestTriggerHistSql, new Object[] { triggerId },
                    new TriggerHistoryMapper());
        } catch (EmptyResultDataAccessException ex) {
            return null;
        }
    }

    class DomainTargetRowMapper implements RowMapper {
        public Object mapRow(ResultSet rs, int num) throws SQLException {
            NodeGroupLink node_groupTarget = new NodeGroupLink();
            node_groupTarget.setSourceGroupId(rs.getString(1));
            node_groupTarget.setTargetGroupId(rs.getString(2));
            node_groupTarget.setDataEventAction(DataEventAction.fromCode(rs
                    .getString(3)));
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
            hist.setLastTriggerBuildReason(TriggerReBuildReason.fromCode(rs
                    .getString(8)));
            hist.setNameForDeleteTrigger(rs.getString(9));
            hist.setNameForInsertTrigger(rs.getString(10));
            hist.setNameForUpdateTrigger(rs.getString(11));
            hist.setSourceSchemaName(rs.getString(12));
            if (this.retMap != null) {
                this.retMap.put((long) hist.getTriggerHistoryId(), hist);
            }
            return hist;
        }
    }

    class TriggerMapper implements RowMapper {
        public Object mapRow(java.sql.ResultSet rs, int arg1)
                throws java.sql.SQLException {
            Trigger trig = new Trigger();
            trig.setTriggerId(rs.getInt("trigger_id"));
            trig.setChannelId(rs.getString("channel_id"));
            trig.setSourceTableName(rs.getString("source_table_name"));
            trig.setTargetTableName(rs.getString("target_table_name"));
            trig.setSourceGroupId(rs.getString("source_node_group_id"));
            trig.setTargetSchemaName(rs.getString("target_schema_name"));
            trig.setSyncOnInsert(rs.getBoolean("sync_on_insert"));
            trig.setSyncOnUpdate(rs.getBoolean("sync_on_update"));
            trig.setSyncOnDelete(rs.getBoolean("sync_on_delete"));
            trig.setNameForDeleteTrigger(rs
                    .getString("name_for_delete_trigger"));
            trig.setNameForInsertTrigger(rs
                    .getString("name_for_insert_trigger"));
            trig.setNameForUpdateTrigger(rs
                    .getString("name_for_update_trigger"));
            String schema = rs.getString("source_schema_name");
            trig.setSourceSchemaName(schema == null ? dbDialect
                    .getDefaultSchema() : schema);
            trig.setTargetGroupId(rs.getString("target_node_group_id"));
            trig.setExcludedColumnNames(rs.getString("excluded_column_names"));
            String condition = rs.getString("sync_on_insert_condition");
            if (condition != null) {
                trig.setSyncOnInsertCondition(condition);
            }
            condition = rs.getString("sync_on_update_condition");
            if (condition != null) {
                trig.setSyncOnUpdateCondition(condition);
            }

            condition = rs.getString("sync_on_delete_condition");
            if (condition != null) {
                trig.setSyncOnDeleteCondition(condition);
            }
            trig.setTxIdExpression(rs.getString("tx_id_expression"));
            trig.setLastModifiedTime(rs.getTimestamp("last_updated_time"));
            trig.setUpdatedBy(rs.getString("last_updated_by"));
            trig.setInitialLoadOrder(rs.getInt("initial_load_order"));
            trig.setInactiveTime(rs.getTimestamp("inactive_time"));
            condition = rs.getString("node_select");
            if (condition != null) {
                trig.setNodeSelect(condition);
            }

            condition = rs.getString("initial_load_select");
            if (condition != null) {
                trig.setInitialLoadSelect(condition);
            }

            return trig;
        }
    }

    public void setTriggerHistSql(String tableSyncAuditSql) {
        this.triggerHistSql = tableSyncAuditSql;
    }

    public void setLatestTriggerHistSql(String latestTableSyncAuditSql) {
        this.latestTriggerHistSql = latestTableSyncAuditSql;
    }

    public void setAllTriggerHistSql(String allTableSyncAuditSql) {
        this.allTriggerHistSql = allTableSyncAuditSql;
    }

    public void setInsertChannelSql(String insertChannelSql) {
        this.insertChannelSql = insertChannelSql;
    }

    public void setGroupsLinksForSql(String groupsTargetsForSql) {
        this.groupsLinksForSql = groupsTargetsForSql;
    }

    public void setInsertTriggerSql(String insertTableSyncConfigSql) {
        this.insertTriggerSql = insertTableSyncConfigSql;
    }

    public void setRootConfigChannelTableNames(
            List<String> configChannelTableNames) {
        this.rootConfigChannelTableNames = configChannelTableNames;
    }

    public void setSelectTriggerSql(String selectTableSyncConfigSql) {
        this.selectTriggerSql = selectTableSyncConfigSql;
    }

    public void setSelectTriggerTargetSql(String selectTableSyncConfigTargetSql) {
        this.selectTriggerTargetSql = selectTableSyncConfigTargetSql;
    }

    public void setSelectGroupTriggersSql(String selectDomainTableConfigsSql) {
        this.selectGroupTriggersSql = selectDomainTableConfigsSql;
    }

    public void setSelectChannelsSql(String selectChannelsSql) {
        this.selectChannelsSql = selectChannelsSql;
    }

    public void setSelectDataEventActionsByIdSql(
            String selectDataEventActionsByIdSql) {
        this.selectDataEventActionsByIdSql = selectDataEventActionsByIdSql;
    }

    public void setInsertTriggerHistorySql(String insertTriggerHistorySql) {
        this.insertTriggerHistorySql = insertTriggerHistorySql;
    }

    public void setActiveTriggersForSourceNodeGroupSql(
            String activeTriggersForSourceNodeGroupSql) {
        this.activeTriggersForSourceNodeGroupSql = activeTriggersForSourceNodeGroupSql;
    }

    public void setInactiveTriggersForSourceNodeGroupSql(
            String inactiveTriggersForSourceNodeGroupSql) {
        this.inactiveTriggersForSourceNodeGroupSql = inactiveTriggersForSourceNodeGroupSql;
    }

    public void setInactivateTriggerHistorySql(String inactiveTriggerHistorySql) {
        this.inactivateTriggerHistorySql = inactiveTriggerHistorySql;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setNodeConfigChannelTableNames(
            List<String> nodeConfigChannelTableNames) {
        this.nodeConfigChannelTableNames = nodeConfigChannelTableNames;
    }

    public List<String> getNodeConfigChannelTableNames() {
        return nodeConfigChannelTableNames;
    }

    public void setSelectTriggerByIdSql(String selectTriggerByIdSql) {
        this.selectTriggerByIdSql = selectTriggerByIdSql;
    }

    public void setActiveTriggersForReloadSql(String activeTriggersForReloadSql) {
        this.activeTriggersForReloadSql = activeTriggersForReloadSql;
    }

}
