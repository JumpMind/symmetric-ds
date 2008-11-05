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
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.mysql.MySqlDbDialect;
import org.jumpmind.symmetric.model.Channel;
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

public class ConfigurationService extends AbstractService implements IConfigurationService {

    final static Log logger = LogFactory.getLog(ConfigurationService.class);

    private static final long MAX_CHANNEL_CACHE_TIME = 60000;
    
    private static List<NodeChannel> channelCache;
    
    private static long channelCacheTime;

    private List<String> rootConfigChannelTableNames;

    private Map<String, String> rootConfigChannelInitialLoadSelect;

    private IDbDialect dbDialect;

    /**
     * Cache the history for performance. History never changes and does not
     * grow big so this should be OK.
     */
    private HashMap<Integer, TriggerHistory> historyMap = new HashMap<Integer, TriggerHistory>();

    protected void initSystemChannels() {
        try {
            jdbcTemplate.update(getSql("insertChannelSql"), new Object[] { Constants.CHANNEL_CONFIG, 0, 100, 100, 1 });
        } catch (DataIntegrityViolationException ex) {
            logger.debug("Channel " + Constants.CHANNEL_CONFIG + " already created.");
        }
        try {
            jdbcTemplate.update(getSql("insertChannelSql"), new Object[] { Constants.CHANNEL_RELOAD, 1, 1, 10, 1 });
        } catch (DataIntegrityViolationException ex) {
            logger.debug("Channel " + Constants.CHANNEL_RELOAD + " already created.");
        }
    }

    public void inactivateTriggerHistory(TriggerHistory history) {
        jdbcTemplate.update(getSql("inactivateTriggerHistorySql"), new Object[] { history.getTriggerHistoryId() });
    }

    @SuppressWarnings("unchecked")
    public List<NodeGroupLink> getGroupLinks() {
        return jdbcTemplate.query(getSql("groupsLinksSql"), new DomainTargetRowMapper());
    }

    @SuppressWarnings("unchecked")
    public List<NodeGroupLink> getGroupLinksFor(String nodeGroupId) {
        return jdbcTemplate.query(getSql("groupsLinksForSql"), new Object[] { nodeGroupId },
                new DomainTargetRowMapper());
    }

    public List<String> getRootConfigChannelTableNames() {
        return rootConfigChannelTableNames;
    }

    public void saveChannel(Channel channel) {
        if (0 == jdbcTemplate.update(getSql("updateChannelSql"), new Object[] { channel.getProcessingOrder(),
                channel.getMaxBatchSize(), channel.getMaxBatchToSend(), channel.isEnabled() ? 1 : 0, channel.getId() })) {
            jdbcTemplate.update(getSql("insertChannelSql"), new Object[] { channel.getId(),
                    channel.getProcessingOrder(), channel.getMaxBatchSize(), channel.getMaxBatchToSend(),
                    channel.isEnabled() ? 1 : 0 });
        }
    }

    public void deleteChannel(Channel channel) {
        jdbcTemplate.update(getSql("deleteChannelSql"), new Object[] { channel.getId() });
    }

    public void initTriggerRowsForConfigChannel() {
        if (StringUtils.isEmpty(parameterService.getRegistrationUrl())) {
            initSystemChannels();
            for (NodeGroupLink link : getGroupLinks()) {
                initTriggerRowsForConfigChannel(link.getSourceGroupId(), link.getTargetGroupId());
            }
        }
    }

    private void initTriggerRowsForConfigChannel(String sourceGroupId, String targetGroupId) {
        int initialLoadOrder = 1;
        for (String tableName : getRootConfigChannelTableNames()) {
            Trigger trigger = getTriggerForTarget(tableName, sourceGroupId, targetGroupId, Constants.CHANNEL_CONFIG);
            if (trigger == null) {
                String initialLoadSelect = rootConfigChannelInitialLoadSelect.get(tableName);
                trigger = new Trigger();
                trigger.setSyncOnDelete(false);
                trigger.setSyncOnInsert(false);
                trigger.setSyncOnUpdate(false);
                trigger.setSourceTableName(tableName);
                trigger.setSourceGroupId(sourceGroupId);
                trigger.setTargetGroupId(targetGroupId);
                trigger.setChannelId(Constants.CHANNEL_CONFIG);
                trigger.setInitialLoadOrder(initialLoadOrder++);
                trigger.setInitialLoadSelect(initialLoadSelect);
                insert(trigger);
            }
        }
    }

    @Deprecated
    public List<NodeChannel> getChannelsFor(boolean failIfTableDoesNotExist) {
        return getChannels();
    }
    
    @SuppressWarnings("unchecked")
    public List<NodeChannel> getChannels() {
        if (System.currentTimeMillis() - channelCacheTime >= MAX_CHANNEL_CACHE_TIME || channelCache == null) {
            channelCache = jdbcTemplate.query(getSql("selectChannelsSql"), new Object[] {}, new RowMapper() {
                public Object mapRow(java.sql.ResultSet rs, int arg1) throws java.sql.SQLException {
                    NodeChannel channel = new NodeChannel();
                    channel.setId(rs.getString(1));
                    channel.setNodeId(rs.getString(2));
                    channel.setIgnored(isSet(rs.getObject(3)));
                    channel.setSuspended(isSet(rs.getObject(4)));
                    channel.setProcessingOrder(rs.getInt(5));
                    channel.setMaxBatchSize(rs.getInt(6));
                    channel.setEnabled(rs.getBoolean(7));
                    channel.setMaxBatchToSend(rs.getInt(8));
                    return channel;
                };
            });
            channelCacheTime = System.currentTimeMillis();
        }
        return channelCache;
    }
    
    public void flushChannels() {
        channelCache = null;
    }

    private boolean isSet(Object value) {
        if (value != null && value.toString().equals("1")) {
            return true;
        } else {
            return false;
        }
    }

    @SuppressWarnings("unchecked")
    public DataEventAction getDataEventActionsByGroupId(String sourceGroupId, String targetGroupId) {
        String code = (String) jdbcTemplate.queryForObject(getSql("selectDataEventActionsByIdSql"), new Object[] {
                sourceGroupId, targetGroupId }, String.class);

        return DataEventAction.fromCode(code);
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerFor(String table, String sourceNodeGroupId) {
        List<Trigger> configs = (List<Trigger>) jdbcTemplate.query(getSql("selectTriggerSql"), new Object[] { table,
                sourceNodeGroupId }, new TriggerMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public List<Trigger> getActiveTriggersForSourceNodeGroup(String sourceNodeGroupId) {
        return (List<Trigger>) jdbcTemplate.query(getSql("activeTriggersForSourceNodeGroupSql"),
                new Object[] { sourceNodeGroupId }, new TriggerMapper());
    }

    @SuppressWarnings("unchecked")
    public List<Trigger> getActiveTriggersForReload(String sourceNodeGroupId, String targetNodeGroupId) {
        return (List<Trigger>) jdbcTemplate.query(getSql("activeTriggersForReloadSql"), new Object[] {
                sourceNodeGroupId, targetNodeGroupId, Constants.CHANNEL_CONFIG }, new TriggerMapper());
    }

    @SuppressWarnings("unchecked")
    public List<Trigger> getInactiveTriggersForSourceNodeGroup(String sourceNodeGroupId) {
        return (List<Trigger>) jdbcTemplate.query(getSql("inactiveTriggersForSourceNodeGroupSql"),
                new Object[] { sourceNodeGroupId }, new TriggerMapper());
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerForTarget(String table, String sourceNodeGroupId, String targetNodeGroupId, String channel) {
        List<Trigger> configs = (List<Trigger>) jdbcTemplate.query(getSql("selectTriggerTargetSql"), new Object[] {
                table, targetNodeGroupId, channel, sourceNodeGroupId }, new TriggerMapper());
        if (configs.size() > 0) {
            return configs.get(0);
        } else {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    public Trigger getTriggerById(int triggerId) {
        List<Trigger> triggers = (List<Trigger>) jdbcTemplate.query(getSql("selectTriggerByIdSql"),
                new Object[] { triggerId }, new TriggerMapper());
        if (triggers.size() > 0) {
            return triggers.get(0);
        } else {
            return null;
        }
    }

    public Map<String, List<Trigger>> getTriggersByChannelFor(String nodeGroupId) {
        final Map<String, List<Trigger>> retMap = new HashMap<String, List<Trigger>>();
        jdbcTemplate.query(getSql("selectGroupTriggersSql"), new Object[] { nodeGroupId }, new TriggerMapper() {
            public Object mapRow(java.sql.ResultSet rs, int arg1) throws java.sql.SQLException {
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
        jdbcTemplate.update(getSql("insertTriggerHistorySql"), new Object[] { newHistRecord.getTriggerId(),
                newHistRecord.getSourceTableName(), newHistRecord.getTableHash(), newHistRecord.getCreateTime(),
                newHistRecord.getColumnNames(), newHistRecord.getPkColumnNames(),
                newHistRecord.getLastTriggerBuildReason().getCode(), newHistRecord.getNameForDeleteTrigger(),
                newHistRecord.getNameForInsertTrigger(), newHistRecord.getNameForUpdateTrigger(),
                newHistRecord.getSourceSchemaName(), newHistRecord.getSourceCatalogName() }, new int[] { Types.INTEGER,
                Types.VARCHAR, Types.BIGINT, Types.TIMESTAMP, Types.VARCHAR, Types.VARCHAR, Types.CHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR });
    }

    public void insert(Trigger trigger) {
        jdbcTemplate.update(getSql("insertTriggerSql"), new Object[] { trigger.getSourceCatalogName(),
                trigger.getSourceSchemaName(), trigger.getSourceTableName(), trigger.getTargetSchemaName(),
                trigger.getTargetTableName(), trigger.getSourceGroupId(), trigger.getTargetGroupId(),
                trigger.getChannelId(), trigger.isSyncOnUpdate() ? 1 : 0, trigger.isSyncOnInsert() ? 1 : 0,
                trigger.isSyncOnDelete() ? 1 : 0, trigger.isSyncOnIncomingBatch() ? 1 : 0,
                trigger.getNameForUpdateTrigger(), trigger.getNameForInsertTrigger(),
                trigger.getNameForDeleteTrigger(), trigger.getSyncOnUpdateCondition(),
                trigger.getSyncOnInsertCondition(), trigger.getSyncOnDeleteCondition(), trigger.getInitialLoadSelect(),
                trigger.getNodeSelect(), trigger.getTxIdExpression(), trigger.getExcludedColumnNames(),
                trigger.getInitialLoadOrder(), new Date(), null, trigger.getUpdatedBy(), new Date() }, new int[] {
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT, Types.SMALLINT,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR,
                Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.VARCHAR, Types.INTEGER, Types.TIMESTAMP,
                Types.TIMESTAMP, Types.VARCHAR, Types.TIMESTAMP });
    }

    public Map<Long, TriggerHistory> getHistoryRecords() {
        final Map<Long, TriggerHistory> retMap = new HashMap<Long, TriggerHistory>();
        jdbcTemplate.query(getSql("allTriggerHistSql"), new TriggerHistoryMapper(retMap));
        return retMap;
    }

    public TriggerHistory getHistoryRecordFor(int auditId) {
        TriggerHistory history = historyMap.get(auditId);
        if (history == null) {
            try {
                history = (TriggerHistory) jdbcTemplate.queryForObject(getSql("triggerHistSql"),
                        new Object[] { auditId }, new TriggerHistoryMapper());
                historyMap.put(auditId, history);
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

    class DomainTargetRowMapper implements RowMapper {
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
            if (this.retMap != null) {
                this.retMap.put((long) hist.getTriggerHistoryId(), hist);
            }
            return hist;
        }
    }

    class TriggerMapper implements RowMapper {
        public Object mapRow(java.sql.ResultSet rs, int arg1) throws java.sql.SQLException {
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
            trig.setSyncOnIncomingBatch(rs.getBoolean("sync_on_incoming_batch"));
            trig.setSyncColumnLevel(rs.getBoolean("sync_column_level"));
            trig.setNameForDeleteTrigger(rs.getString("name_for_delete_trigger"));
            trig.setNameForInsertTrigger(rs.getString("name_for_insert_trigger"));
            trig.setNameForUpdateTrigger(rs.getString("name_for_update_trigger"));
            String schema = rs.getString("source_schema_name");
            trig.setSourceSchemaName(schema);
            String catalog = rs.getString("source_catalog_name");
            if (catalog == null && schema != null && dbDialect instanceof MySqlDbDialect) {
                // set catalog == schema for backwards compatibility ... remove
                // this in version 2.0
                catalog = schema;
            }
            trig.setSourceCatalogName(catalog);
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

    public boolean isRegistrationServer() {
        return StringUtils.isBlank(parameterService.getRegistrationUrl());
    }

    public void setRootConfigChannelTableNames(List<String> configChannelTableNames) {
        this.rootConfigChannelTableNames = configChannelTableNames;
    }

    public void setRootConfigChannelInitialLoadSelect(Map<String, String> rootConfigChannelInitialLoadSelect) {
        this.rootConfigChannelInitialLoadSelect = rootConfigChannelInitialLoadSelect;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}
