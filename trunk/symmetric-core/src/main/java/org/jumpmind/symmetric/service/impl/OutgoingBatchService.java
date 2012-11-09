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

package org.jumpmind.symmetric.service.impl;

import java.sql.Types;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.TableConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeGroupChannelWindow;
import org.jumpmind.symmetric.model.NodeHost;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatchSummary;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ISequenceService;
import org.jumpmind.util.AppUtils;
import org.jumpmind.util.FormatUtils;

/**
 * @see IOutgoingBatchService
 */
public class OutgoingBatchService extends AbstractService implements IOutgoingBatchService {

    private INodeService nodeService;

    private IConfigurationService configurationService;

    private ISequenceService sequenceService;
    
    private IClusterService clusterService;

    public OutgoingBatchService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, INodeService nodeService,
            IConfigurationService configurationService, ISequenceService sequenceService, IClusterService clusterService) {
        super(parameterService, symmetricDialect);
        this.nodeService = nodeService;
        this.configurationService = configurationService;
        this.sequenceService = sequenceService;
        this.clusterService = clusterService;
        setSqlMap(new OutgoingBatchServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public void markAllAsSentForNode(Node node) {
        OutgoingBatches batches = null;
        do {
            batches = getOutgoingBatches(node, true);
            for (OutgoingBatch outgoingBatch : batches.getBatches()) {
                outgoingBatch.setStatus(Status.OK);
                outgoingBatch.setErrorFlag(false);
                updateOutgoingBatch(outgoingBatch);
            }
        } while (batches.getBatches().size() > 0);
    }

    public void updateAbandonedRoutingBatches() {
        sqlTemplate.update(getSql("updateOutgoingBatchesStatusSql"), Status.NE.name(),
                Status.RT.name());
    }

    public void updateOutgoingBatches(List<OutgoingBatch> outgoingBatches) {
        for (OutgoingBatch batch : outgoingBatches) {
            updateOutgoingBatch(batch);
        }
    }

    public void updateOutgoingBatch(OutgoingBatch outgoingBatch) {
        outgoingBatch.setLastUpdatedTime(new Date());
        outgoingBatch.setLastUpdatedHostName(clusterService.getServerId());
        sqlTemplate.update(
                getSql("updateOutgoingBatchSql"),
                new Object[] { outgoingBatch.getStatus().name(),
                        outgoingBatch.isLoadFlag() ? 1 : 0, outgoingBatch.isErrorFlag() ? 1 : 0,
                        outgoingBatch.getByteCount(), outgoingBatch.getExtractCount(),
                        outgoingBatch.getSentCount(), outgoingBatch.getLoadCount(),
                        outgoingBatch.getDataEventCount(), outgoingBatch.getReloadEventCount(),
                        outgoingBatch.getInsertEventCount(), outgoingBatch.getUpdateEventCount(),
                        outgoingBatch.getDeleteEventCount(), outgoingBatch.getOtherEventCount(),
                        outgoingBatch.getIgnoreCount(), outgoingBatch.getRouterMillis(),
                        outgoingBatch.getNetworkMillis(), outgoingBatch.getFilterMillis(),
                        outgoingBatch.getLoadMillis(), outgoingBatch.getExtractMillis(),
                        outgoingBatch.getSqlState(), outgoingBatch.getSqlCode(),
                        FormatUtils.abbreviateForLogging(outgoingBatch.getSqlMessage()),
                        outgoingBatch.getFailedDataId(), outgoingBatch.getLastUpdatedHostName(),
                        outgoingBatch.getLastUpdatedTime(), outgoingBatch.getBatchId(),
                        outgoingBatch.getNodeId() },
                new int[] { Types.CHAR, Types.NUMERIC, Types.NUMERIC, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                        Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.VARCHAR,
                        Types.NUMERIC, Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.TIMESTAMP,
                        Types.NUMERIC, Types.VARCHAR });
    }

    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch) {
        ISqlTransaction transaction = null;
        try {
            transaction = sqlTemplate.startSqlTransaction();
            insertOutgoingBatch(transaction, outgoingBatch);
            transaction.commit();
        } finally {
            close(transaction);
        }
    }

    public void insertOutgoingBatch(ISqlTransaction transaction, OutgoingBatch outgoingBatch) {
        outgoingBatch.setLastUpdatedHostName(clusterService.getServerId());

        long batchId = outgoingBatch.getBatchId();
        if (batchId <= 0) {
            batchId = sequenceService.nextVal(transaction,TableConstants.SYM_OUTGOING_BATCH);
        }
        transaction.prepareAndExecute(getSql("insertOutgoingBatchSql"), batchId, outgoingBatch
                .getNodeId(), outgoingBatch.getChannelId(), outgoingBatch.getStatus().name(),
                outgoingBatch.isLoadFlag() ? 1 : 0, outgoingBatch.isCommonFlag() ? 1 : 0,
                outgoingBatch.getReloadEventCount(), outgoingBatch.getOtherEventCount(),
                outgoingBatch.getLastUpdatedHostName());
        outgoingBatch.setBatchId(batchId);
    }

    public OutgoingBatch findOutgoingBatch(long batchId, String nodeId) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "findOutgoingBatchSql"),
                new OutgoingBatchMapper(true, false), new Object[] { batchId, nodeId }, new int[] {
                        Types.NUMERIC, Types.VARCHAR });
        if (list != null && list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }

    public int countOutgoingBatchesInError() {
        return sqlTemplate.queryForInt(getSql("countOutgoingBatchesErrorsSql"));
    }
    
    public int countOutgoingBatchesUnsent() {        
        return sqlTemplate.queryForInt(getSql("countOutgoingBatchesUnsentSql"));
    }

    public int countOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses) {
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("NODES", nodeIds);
        params.put("CHANNELS", channels);
        params.put("STATUSES", toStringList(statuses));

        return sqlTemplate.queryForInt(
                getSql("selectCountBatchesPrefixSql", buildBatchWhere(nodeIds, channels, statuses)),
                params);
    }

    public List<OutgoingBatch> listOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses, long startAtBatchId, final int maxRowsToRetrieve,
            boolean ascending) {

        String where = buildBatchWhere(nodeIds, channels, statuses);
        Map<String, Object> params = new HashMap<String, Object>();
        params.put("NODES", nodeIds);
        params.put("CHANNELS", channels);
        params.put("STATUSES", toStringList(statuses));
        String startAtBatchIdSql = null;
        if (startAtBatchId > 0) {
            if (StringUtils.isBlank(where)) {
                where = " where 1=1 ";
            }
            params.put("BATCH_ID", startAtBatchId);
            if (ascending) {
                startAtBatchIdSql = " and batch_id > :BATCH_ID ";
            } else {
                startAtBatchIdSql = " and batch_id < :BATCH_ID ";
            }
        }

        String sql = getSql("selectOutgoingBatchPrefixSql", where, startAtBatchIdSql,
                ascending ? "order by batch_id asc" : " order by batch_id desc");
        return sqlTemplate.query(sql, maxRowsToRetrieve, new OutgoingBatchMapper(true, false),
                params);

    }

    protected List<String> toStringList(List<OutgoingBatch.Status> statuses) {
        List<String> statusStrings = new ArrayList<String>(statuses.size());
        for (Status status : statuses) {
            statusStrings.add(status.name());
        }
        return statusStrings;

    }

    protected boolean containsOnlyStatus(OutgoingBatch.Status status,
            List<OutgoingBatch.Status> statuses) {
        return statuses.size() == 1 && statuses.get(0) == status;
    }
    
    /**
     * Select batches to process. Batches that are NOT in error will be returned
     * first. They will be ordered by batch id as the batches will have already
     * been created by {@link #buildOutgoingBatches(String)} in channel priority
     * order.
     */
    public OutgoingBatches getOutgoingBatches(Node node, boolean includeDisabledChannels) {
        long ts = System.currentTimeMillis();
        final int maxNumberOfBatchesToSelect = parameterService.getInt(
                ParameterConstants.OUTGOING_BATCH_MAX_BATCHES_TO_SELECT, 1000);
        List<OutgoingBatch> list = (List<OutgoingBatch>) sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "selectOutgoingBatchSql"),
                maxNumberOfBatchesToSelect, new OutgoingBatchMapper(includeDisabledChannels, true),
                new Object[] { node.getNodeId(), OutgoingBatch.Status.NE.name(),
                        OutgoingBatch.Status.QY.name(), OutgoingBatch.Status.SE.name(),
                        OutgoingBatch.Status.LD.name(), OutgoingBatch.Status.ER.name(),
                        OutgoingBatch.Status.IG.name() }, null);

        OutgoingBatches batches = new OutgoingBatches(list);

        List<NodeChannel> channels = configurationService.getNodeChannels(node.getNodeId(), true);
        batches.sortChannels(channels);

        List<OutgoingBatch> keepers = new ArrayList<OutgoingBatch>();

        for (NodeChannel channel : channels) {
            if (parameterService.is(ParameterConstants.DATA_EXTRACTOR_ENABLED)
                    || channel.getChannelId().equals(Constants.CHANNEL_CONFIG)) {
                keepers.addAll(getBatchesForChannelWindows(batches,
                        node,
                        channel,
                        configurationService.getNodeGroupChannelWindows(
                                parameterService.getNodeGroupId(), channel.getChannelId())));
            }
        }
        batches.setBatches(keepers);

        long executeTimeInMs = System.currentTimeMillis() - ts;
        if (executeTimeInMs > Constants.LONG_OPERATION_THRESHOLD) {
            log.warn("{} took {} ms", "selecting batches to extract", executeTimeInMs);
        }

        return batches;
    }
    
    public List<OutgoingBatch> getBatchesForChannelWindows(OutgoingBatches batches, Node targetNode, NodeChannel channel,
            List<NodeGroupChannelWindow> windows) {
        List<OutgoingBatch> keeping = new ArrayList<OutgoingBatch>();
        List<OutgoingBatch> current = batches.getBatches();
        if (current != null && current.size() > 0) {
            if (inTimeWindow(windows, targetNode)) {
                int maxBatchesToSend = channel.getMaxBatchToSend();
                for (OutgoingBatch outgoingBatch : current) {
                    if (channel.getChannelId().equals(outgoingBatch.getChannelId()) && maxBatchesToSend > 0) {
                        keeping.add(outgoingBatch);
                        maxBatchesToSend--;
                    }
                }
            }
        }
        return keeping;
    }

    /**
     * If {@link NodeGroupChannelWindow}s are defined for this channel, then
     * check to see if the time (according to the offset passed in) is within on
     * of the configured windows.
     */
    public boolean inTimeWindow(List<NodeGroupChannelWindow> windows, Node targetNode) {
        if (windows != null && windows.size() > 0) {
            for (NodeGroupChannelWindow window : windows) {
                String timezoneOffset = null;
                List<NodeHost> hosts = nodeService.findNodeHosts(targetNode.getNodeId());
                if (hosts.size() > 0) {
                    timezoneOffset = hosts.get(0).getTimezoneOffset();
                } else {
                    timezoneOffset = AppUtils.getTimezoneOffset();
                }
                if (window.inTimeWindow(timezoneOffset)) {
                    return true;
                }
            }
            return false;
        } else {
            return true;
        }

    }

    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "selectOutgoingBatchRangeSql"),
                new OutgoingBatchMapper(true, false), Long.parseLong(startBatchId),
                Long.parseLong(endBatchId)));
        return batches;
    }

    public OutgoingBatches getOutgoingBatchErrors(int maxRows) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "selectOutgoingBatchErrorsSql"), maxRows,
                new OutgoingBatchMapper(true, false), null, null));
        return batches;
    }

    public boolean isInitialLoadComplete(String nodeId) {
        return areAllLoadBatchesComplete(nodeId)
                && !isUnsentDataOnChannelForNode(Constants.CHANNEL_CONFIG, nodeId);
    }

    public boolean areAllLoadBatchesComplete(String nodeId) {

        NodeSecurity security = nodeService.findNodeSecurity(nodeId);
        if (security == null || security.isInitialLoadEnabled()) {
            return false;
        }

        List<String> statuses = (List<String>) sqlTemplate.query(getSql("initialLoadStatusSql"),
                new StringMapper(), nodeId, 1);
        if (statuses == null || statuses.size() == 0) {
            throw new RuntimeException("The initial load has not been started for " + nodeId);
        }

        for (String status : statuses) {
            if (!Status.OK.name().equals(status)) {
                return false;
            }
        }
        return true;
    }

    public boolean isUnsentDataOnChannelForNode(String channelId, String nodeId) {
        int unsentCount = sqlTemplate.queryForInt(getSql("unsentBatchesForNodeIdChannelIdSql"),
                new Object[] { nodeId, channelId });
        if (unsentCount > 0) {
            return true;
        }

        // Do we need to check for unbatched data?
        return false;
    }

    public List<OutgoingBatchSummary> findOutgoingBatchSummary(Status... statuses) {
        Object[] args = new Object[statuses.length];
        StringBuilder inList = new StringBuilder();
        for (int i = 0; i < statuses.length; i++) {
            args[i] = statuses[i].name();
            inList.append("?,");
        }

        String sql = getSql("selectOutgoingBatchSummaryByStatusSql").replace(":STATUS_LIST",
                inList.substring(0, inList.length() - 1));

        return sqlTemplate.query(sql, new OutgoingBatchSummaryMapper(), args);
    }

    class OutgoingBatchSummaryMapper implements ISqlRowMapper<OutgoingBatchSummary> {
        public OutgoingBatchSummary mapRow(Row rs) {
            OutgoingBatchSummary summary = new OutgoingBatchSummary();
            summary.setBatchCount(rs.getInt("batches"));
            summary.setDataCount(rs.getInt("data"));
            summary.setStatus(Status.valueOf(rs.getString("status")));
            summary.setNodeId(rs.getString("node_id"));
            summary.setOldestBatchCreateTime(rs.getDateTime("oldest_batch_time"));
            return summary;
        }
    }

    class OutgoingBatchMapper implements ISqlRowMapper<OutgoingBatch> {

        private boolean includeDisabledChannels = false;
        private boolean limitBasedOnMaxBatchToSend = false;
        private Map<String, Channel> channels;
        private Map<String, Integer> countByChannel;

        public OutgoingBatchMapper(boolean includeDisabledChannels,
                boolean limitBasedOnMaxBatchToSend) {
            this.includeDisabledChannels = includeDisabledChannels;
            this.limitBasedOnMaxBatchToSend = limitBasedOnMaxBatchToSend;
            this.channels = configurationService.getChannels(false);
            this.countByChannel = new HashMap<String, Integer>();
        }

        public OutgoingBatch mapRow(Row rs) {
            String channelId = rs.getString("channel_id");
            Channel channel = channels.get(channelId);
            Integer count = countByChannel.get(channelId);
            if (count == null) {
                count = 0;
            }
            if (channel != null && (includeDisabledChannels || channel.isEnabled())
                    && (!limitBasedOnMaxBatchToSend || count <= channel.getMaxBatchToSend())) {
                count++;
                OutgoingBatch batch = new OutgoingBatch();
                batch.setChannelId(channelId);
                batch.setNodeId(rs.getString("node_id"));
                batch.setStatus(rs.getString("status"));
                batch.setByteCount(rs.getLong("byte_count"));
                batch.setExtractCount(rs.getLong("extract_count"));
                batch.setSentCount(rs.getLong("sent_count"));
                batch.setLoadCount(rs.getLong("load_count"));
                batch.setDataEventCount(rs.getLong("data_event_count"));
                batch.setReloadEventCount(rs.getLong("reload_event_count"));
                batch.setInsertEventCount(rs.getLong("insert_event_count"));
                batch.setUpdateEventCount(rs.getLong("update_event_count"));
                batch.setDeleteEventCount(rs.getLong("delete_event_count"));
                batch.setOtherEventCount(rs.getLong("other_event_count"));
                batch.setIgnoreCount(rs.getLong("ignore_count"));
                batch.setRouterMillis(rs.getLong("router_millis"));
                batch.setNetworkMillis(rs.getLong("network_millis"));
                batch.setFilterMillis(rs.getLong("filter_millis"));
                batch.setLoadMillis(rs.getLong("load_millis"));
                batch.setExtractMillis(rs.getLong("extract_millis"));
                batch.setSqlState(rs.getString("sql_state"));
                batch.setSqlCode(rs.getInt("sql_code"));
                batch.setSqlMessage(rs.getString("sql_message"));
                batch.setFailedDataId(rs.getLong("failed_data_id"));
                batch.setLastUpdatedHostName(rs.getString("last_update_hostname"));
                batch.setLastUpdatedTime(rs.getDateTime("last_update_time"));
                batch.setCreateTime(rs.getDateTime("create_time"));
                batch.setBatchId(rs.getLong("batch_id"));
                batch.setLoadFlag(rs.getBoolean("load_flag"));
                batch.setErrorFlag(rs.getBoolean("error_flag"));
                batch.setCommonFlag(rs.getBoolean("common_flag"));
                return batch;
            } else {
                return null;
            }
        }
    }

}
