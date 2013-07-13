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
package org.jumpmind.symmetric.service.impl;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlConstants;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.writer.MultiBatchStagingWriter;
import org.jumpmind.symmetric.io.stage.IStagingManager;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.ExtractRequest;
import org.jumpmind.symmetric.model.ExtractRequest.ExtractStatus;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.NodeCommunication;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;
import org.jumpmind.symmetric.model.ProcessInfo;
import org.jumpmind.symmetric.model.ProcessInfoKey;
import org.jumpmind.symmetric.model.RemoteNodeStatus;
import org.jumpmind.symmetric.model.RemoteNodeStatuses;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.ClusterConstants;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IConfigurationService;
import org.jumpmind.symmetric.service.IDataExtractorService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.IGroupletService;
import org.jumpmind.symmetric.service.IInitialLoadExtractorService;
import org.jumpmind.symmetric.service.INodeCommunicationService;
import org.jumpmind.symmetric.service.INodeCommunicationService.INodeCommunicationExecutor;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IOutgoingBatchService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IPurgeService;
import org.jumpmind.symmetric.service.ISequenceService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;

public class InitialLoadExtractorService extends AbstractService implements
        IInitialLoadExtractorService, INodeCommunicationExecutor {

    private IDataService dataService;

    private IDataExtractorService dataExtractorService;

    private INodeCommunicationService nodeCommunicationService;

    private IClusterService clusterService;

    private INodeService nodeService;

    private ITriggerRouterService triggerRouterService;

    private IConfigurationService configurationService;

    private IOutgoingBatchService outgoingBatchService;

    private ISequenceService sequenceService;

    private IGroupletService groupletService;

    private IStatisticManager statisticManager;

    private IPurgeService purgeService;

    private IStagingManager stagingManager;

    private boolean syncTriggersBeforeInitialLoadAttempted = false;

    public InitialLoadExtractorService(IParameterService parameterService,
            ISymmetricDialect symmetricDialect, IDataExtractorService dataExtractorService,
            IDataService dataService, INodeCommunicationService nodeCommunicationService,
            IClusterService clusterService, INodeService nodeService,
            ITriggerRouterService triggerRouterService, IConfigurationService configurationService,
            IOutgoingBatchService outgoingBatchService, ISequenceService sequenceService,
            IGroupletService groupletService, IStatisticManager statisticManager,
            IPurgeService purgeService, IStagingManager stagingManager) {
        super(parameterService, symmetricDialect);
        this.dataService = dataService;
        this.dataExtractorService = dataExtractorService;
        this.nodeCommunicationService = nodeCommunicationService;
        this.clusterService = clusterService;
        this.nodeService = nodeService;
        this.triggerRouterService = triggerRouterService;
        this.configurationService = configurationService;
        this.outgoingBatchService = outgoingBatchService;
        this.sequenceService = sequenceService;
        this.groupletService = groupletService;
        this.statisticManager = statisticManager;
        this.purgeService = purgeService;
        this.stagingManager = stagingManager;
        setSqlMap(new InitialLoadExtractorSqlMap(symmetricDialect.getPlatform(),
                createSqlReplacementTokens()));
    }

    public RemoteNodeStatuses queueWork(boolean force) {
        final RemoteNodeStatuses statuses = new RemoteNodeStatuses();
        Node identity = nodeService.findIdentity();
        if (identity != null) {
            if (force || clusterService.lock(ClusterConstants.INITIAL_LOAD_EXTRACT)) {
                try {
                    List<String> nodeIds = getExtractRequestNodes();
                    for (String nodeId : nodeIds) {
                        queue(nodeId, statuses);
                    }
                } finally {
                    if (!force) {
                        clusterService.unlock(ClusterConstants.INITIAL_LOAD_EXTRACT);
                    }
                }
            }
        } else {
            log.debug("Not running initial load extract service because this node does not have an identity");
        }
        return statuses;
    }

    protected void queue(String nodeId, RemoteNodeStatuses statuses) {
        final NodeCommunication.CommunicationType TYPE = NodeCommunication.CommunicationType.INITIAL_LOAD_EXTRACT;
        int availableThreads = nodeCommunicationService.getAvailableThreads(TYPE);
        NodeCommunication lock = nodeCommunicationService.find(nodeId, TYPE);
        if (availableThreads > 0) {
            nodeCommunicationService.execute(lock, statuses, this);
        }
    }

    public List<String> getExtractRequestNodes() {
        return sqlTemplate.query(getSql("selectNodeIdsForExtractSql"), SqlConstants.STRING_MAPPER,
                ExtractStatus.NE.name());
    }

    public List<ExtractRequest> getExtractRequestsForNode(String nodeId) {
        return sqlTemplate.query(getSql("selectExtractRequestForNodeSql"),
                new ExtractRequestMapper(), nodeId, Status.NE.name());
    }

    public void requestExtractRequest(String nodeId, TriggerRouter triggerRouter,
            long startBatchId, long endBatchId) {
    }

    public void execute(NodeCommunication nodeCommunication, RemoteNodeStatus status) {

        List<ExtractRequest> requests = getExtractRequestsForNode(nodeCommunication.getNodeId());
        if (requests.size() > 0) {
            Node identity = nodeService.findIdentity();
            Node targetNode = nodeService.findNode(nodeCommunication.getNodeId());
            ExtractRequest request = requests.get(0);
            List<OutgoingBatch> batches = outgoingBatchService.getOutgoingBatchRange(
                    request.getStartBatchId(), request.getEndBatchId()).getBatches();
            ProcessInfo processInfo = statisticManager.newProcessInfo(new ProcessInfoKey(identity
                    .getNodeId(), nodeCommunication.getNodeId(),
                    ProcessInfoKey.ProcessType.INITIAL_LOAD_EXTRACT_JOB));
            List<OutgoingBatch> oneBatch = new ArrayList<OutgoingBatch>(1);
            oneBatch.add(batches.get(0));
            Channel channel = configurationService.getChannel(batches.get(0).getChannelId());
            /* "trick" the extractor to extract one reload batch, but we will split it among the X batches
            when writing it */
            dataExtractorService.extract(processInfo, targetNode, oneBatch,
                    new MultiBatchStagingWriter(identity.getNodeId(),
                            Constants.STAGING_CATEGORY_OUTGOING, stagingManager,
                            toBatchIds(batches), channel.getMaxBatchSize()), true);
        } else {
            log.warn("An extract was requested, but no extract records where found for node {}",
                    nodeCommunication.getNodeId());
        }

        // TODO:         
        // in the data extractor service if the extract_job_flag=1 and the status is RQ then don't stream any batches
        // if extract_job_flag=1 and the status is NE  and the batch is not staged then update the request back to a status of NE

        // TODO: update the purge service to purge extract requests

    }

    protected long[] toBatchIds(List<OutgoingBatch> batches) {
        long[] batchIds = new long[batches.size()];
        int index = 0;
        for (OutgoingBatch outgoingBatch : batches) {
            batchIds[index++] = outgoingBatch.getBatchId();
        }
        return batchIds;
    }

    class ExtractRequestMapper implements ISqlRowMapper<ExtractRequest> {
        public ExtractRequest mapRow(Row row) {
            ExtractRequest request = new ExtractRequest();
            request.setNodeId(row.getString("node_id"));
            request.setRequestId(row.getLong("request_id"));
            request.setStartBatchId(row.getLong("start_batch_id"));
            request.setEndBatchId(row.getLong("end_batch_id"));
            request.setStatus(ExtractStatus.valueOf(row.getString("status").toUpperCase()));
            request.setCreateTime(row.getDateTime("create_time"));
            request.setLastUpdateTime(row.getDateTime("last_update_time"));
            return request;
        }
    }

}
