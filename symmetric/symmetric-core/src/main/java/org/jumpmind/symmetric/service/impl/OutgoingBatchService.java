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
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.sql.AbstractSqlMap;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlList;
import org.jumpmind.db.sql.SqlToken;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.log.Log;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeChannel;
import org.jumpmind.symmetric.model.NodeSecurity;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.OutgoingBatchSummary;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.AppUtils;
import org.springframework.transaction.annotation.Transactional;

/**
 * @see IOutgoingBatchService
 */
public class OutgoingBatchService extends AbstractService implements IOutgoingBatchService {

    private INodeService nodeService;

    private IConfigurationService configurationService;

    public OutgoingBatchService(Log log, IParameterService parameterService,
            ISymmetricDialect symmetricDialect, INodeService nodeService,
            IConfigurationService configurationService) {
        super(log, parameterService, symmetricDialect);
        this.nodeService = nodeService;
        this.configurationService = configurationService;
    }

    @Override
    protected AbstractSqlMap createSqlMap() {
        return new OutgoingBatchServiceSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens());
    }

    @Transactional
    public void markAllAsSentForNode(Node node) {
        OutgoingBatches batches = null;
        do {
            batches = getOutgoingBatches(node);
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
        outgoingBatch.setLastUpdatedHostName(AppUtils.getServerId());
        sqlTemplate
                .update(getSql("updateOutgoingBatchSql"),
                        new Object[] { outgoingBatch.getStatus().name(),
                                outgoingBatch.isLoadFlag() ? 1 : 0,
                                outgoingBatch.isErrorFlag() ? 1 : 0, outgoingBatch.getByteCount(),
                                outgoingBatch.getExtractCount(), outgoingBatch.getSentCount(),
                                outgoingBatch.getLoadCount(), outgoingBatch.getDataEventCount(),
                                outgoingBatch.getReloadEventCount(),
                                outgoingBatch.getInsertEventCount(),
                                outgoingBatch.getUpdateEventCount(),
                                outgoingBatch.getDeleteEventCount(),
                                outgoingBatch.getOtherEventCount(),
                                outgoingBatch.getRouterMillis(), outgoingBatch.getNetworkMillis(),
                                outgoingBatch.getFilterMillis(), outgoingBatch.getLoadMillis(),
                                outgoingBatch.getExtractMillis(), outgoingBatch.getSqlState(),
                                outgoingBatch.getSqlCode(),
                                StringUtils.abbreviate(outgoingBatch.getSqlMessage(), 1000),
                                outgoingBatch.getFailedDataId(),
                                outgoingBatch.getLastUpdatedHostName(),
                                outgoingBatch.getLastUpdatedTime(), outgoingBatch.getBatchId() },
                        new int[] { Types.CHAR, Types.NUMERIC, Types.NUMERIC, Types.BIGINT,
                                Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                                Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                                Types.BIGINT, Types.BIGINT, Types.BIGINT, Types.BIGINT,
                                Types.BIGINT, Types.BIGINT, Types.VARCHAR, Types.NUMERIC,
                                Types.VARCHAR, Types.BIGINT, Types.VARCHAR, Types.TIMESTAMP,
                                Types.NUMERIC });
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
        outgoingBatch.setLastUpdatedHostName(AppUtils.getServerId());
        long batchId = transaction.insertWithGeneratedKey(getSql("insertOutgoingBatchSql"),
                symmetricDialect.getSequenceKeyName(SequenceIdentifier.OUTGOING_BATCH),
                symmetricDialect.getSequenceName(SequenceIdentifier.OUTGOING_BATCH), outgoingBatch
                        .getNodeId(), outgoingBatch.getChannelId(), outgoingBatch.getStatus()
                        .name(), outgoingBatch.isLoadFlag() ? 1 : 0, outgoingBatch
                        .getReloadEventCount(), outgoingBatch.getOtherEventCount(), outgoingBatch
                        .getLastUpdatedHostName());
        outgoingBatch.setBatchId(batchId);
    }

    public OutgoingBatch findOutgoingBatch(long batchId) {
        List<OutgoingBatch> list = (List<OutgoingBatch>) sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "findOutgoingBatchSql"),
                new OutgoingBatchMapper(), new Object[] { batchId }, new int[] { Types.NUMERIC });
        if (list != null && list.size() > 0) {
            return list.get(0);
        } else {
            return null;
        }
    }

    public int countOutgoingBatchesInError() {
        return sqlTemplate.queryForInt(getSql("countOutgoingBatchesErrorsSql"));
    }

    public int countOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses) {
        if (nodeIds.size() > 0 && channels.size() > 0 && statuses.size() > 0) {
            return sqlTemplate
                    .queryForInt(
                            getSql("selectCountBatchesPrefixSql",
                                    containsOnlyErrorStatus(statuses) ? "selectOutgoingBatchByChannelWithErrorSql"
                                            : "selectOutgoingBatchByChannelAndStatusSql"),
                            new SqlList(":NODES", nodeIds), new SqlList(":CHANNELS", channels),
                            toStringList(":STATUSES", statuses));
        } else {
            return 0;
        }
    }

    public List<OutgoingBatch> listOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses, long startAtBatchId, final int maxRowsToRetrieve) {
        if (nodeIds.size() > 0 && channels.size() > 0 && statuses.size() > 0) {
            String startAtBatchIdSql = null;
            List<Object> args = new ArrayList<Object>();
            args.add(new SqlList(":NODES", nodeIds));
            args.add(new SqlList(":CHANNELS", channels));
            args.add(toStringList(":STATUSES", statuses));

            if (startAtBatchId > 0) {
                args.add(new SqlToken(":BATCH_ID", startAtBatchId));
                startAtBatchIdSql = " and batch_id < :BATCH_ID ";
            }
            String sql = getSql("selectOutgoingBatchPrefixSql",
                    containsOnlyErrorStatus(statuses) ? "selectOutgoingBatchByChannelWithErrorSql"
                            : "selectOutgoingBatchByChannelAndStatusSql", startAtBatchIdSql,
                    " order by batch_id desc");

            return sqlTemplate.query(sql, maxRowsToRetrieve, new OutgoingBatchMapper(),
                    args.toArray(new Object[args.size()]), null);
        } else {
            return new ArrayList<OutgoingBatch>(0);
        }
    }

    protected boolean containsOnlyErrorStatus(List<OutgoingBatch.Status> statuses) {
        return statuses.size() == 1 && statuses.get(0) == OutgoingBatch.Status.ER;
    }

    protected SqlList toStringList(String replacementToken, List<OutgoingBatch.Status> statuses) {
        SqlList statusStrings = new SqlList(replacementToken, statuses.size());
        for (Status status : statuses) {
            statusStrings.add(status.name());
        }
        return statusStrings;

    }

    /**
     * Select batches to process. Batches that are NOT in error will be returned
     * first. They will be ordered by batch id as the batches will have already
     * been created by {@link #buildOutgoingBatches(String)} in channel priority
     * order.
     */
    public OutgoingBatches getOutgoingBatches(Node node) {
        long ts = System.currentTimeMillis();
        final int maxNumberOfBatchesToSelect = parameterService.getInt(
                ParameterConstants.OUTGOING_BATCH_MAX_BATCHES_TO_SELECT, 1000);
        List<OutgoingBatch> list = (List<OutgoingBatch>) sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "selectOutgoingBatchSql"),
                maxNumberOfBatchesToSelect, new OutgoingBatchMapper(),
                new Object[] { node.getNodeId(), OutgoingBatch.Status.NE.name(),
                        OutgoingBatch.Status.QY.name(), OutgoingBatch.Status.SE.name(),
                        OutgoingBatch.Status.LD.name(), OutgoingBatch.Status.ER.name() }, null);

        OutgoingBatches batches = new OutgoingBatches(list);

        List<NodeChannel> channels = configurationService.getNodeChannels(node.getNodeId(), true);
        batches.sortChannels(channels);

        List<OutgoingBatch> keepers = new ArrayList<OutgoingBatch>();

        for (NodeChannel channel : channels) {
            if (parameterService.is(ParameterConstants.DATA_EXTRACTOR_ENABLED)
                    || channel.getChannelId().equals(Constants.CHANNEL_CONFIG)) {
                keepers.addAll(batches.getBatchesForChannelWindows(
                        node,
                        channel,
                        configurationService.getNodeGroupChannelWindows(
                                parameterService.getNodeGroupId(), channel.getChannelId())));
            }
        }
        batches.setBatches(keepers);

        long executeTimeInMs = System.currentTimeMillis() - ts;
        if (executeTimeInMs > Constants.LONG_OPERATION_THRESHOLD) {
            log.warn("%s took %d ms", "selecting batches to extract", executeTimeInMs);
        }

        return batches;
    }

    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "selectOutgoingBatchRangeSql"),
                new OutgoingBatchMapper(), Long.parseLong(startBatchId), Long.parseLong(endBatchId)));
        return batches;
    }

    public OutgoingBatches getOutgoingBatchErrors(int maxRows) {
        OutgoingBatches batches = new OutgoingBatches();
        batches.setBatches(sqlTemplate.query(
                getSql("selectOutgoingBatchPrefixSql", "selectOutgoingBatchErrorsSql"), maxRows,
                new OutgoingBatchMapper(), null, null));
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
        public OutgoingBatch mapRow(Row rs) {
            OutgoingBatch batch = new OutgoingBatch();
            batch.setNodeId(rs.getString("node_id"));
            batch.setChannelId(rs.getString("channel_id"));
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
            batch.setRouterMillis(rs.getLong("router_millis"));
            batch.setNetworkMillis(rs.getLong("network_millis"));
            batch.setFilterMillis(rs.getLong("filter_millis"));
            batch.setLoadMillis(rs.getLong("load_millis"));
            batch.setExtractMillis(rs.getLong("extract_millis"));
            batch.setSqlState(rs.getString("sql_state"));
            batch.setSqlCode(rs.getInt("sql_code"));
            batch.setSqlMessage(rs.getString("sql_message"));
            batch.setFailedDataId(rs.getLong("failed_data_id"));
            batch.setLastUpdatedHostName(rs.getString("last_update_host_name"));
            batch.setLastUpdatedTime(rs.getDateTime("last_update_time"));
            batch.setCreateTime(rs.getDateTime("create_time"));
            batch.setBatchId(rs.getLong("batch_id"));
            batch.setLoadFlag(rs.getBoolean("load_flag"));
            batch.setErrorFlag(rs.getBoolean("error_flag"));
            return batch;
        }
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setConfigurationService(IConfigurationService configurationService) {
        this.configurationService = configurationService;
    }

}