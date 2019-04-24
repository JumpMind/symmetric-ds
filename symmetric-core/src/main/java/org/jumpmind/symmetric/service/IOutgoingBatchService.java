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

package org.jumpmind.symmetric.service;

import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.model.AbstractBatch.Status;
import org.jumpmind.symmetric.model.LoadSummary;
import org.jumpmind.symmetric.model.NodeGroupLinkAction;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchSummary;
import org.jumpmind.symmetric.model.OutgoingBatches;
import org.jumpmind.symmetric.model.OutgoingLoadSummary;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService.LoadCounts;
import org.jumpmind.symmetric.service.impl.OutgoingBatchService.LoadStatusSummary;

/**
 * This service provides an API to access to the outgoing batch table. 
 */
public interface IOutgoingBatchService {
    
    public List<String> getNodesInError();

    public void markAllAsSentForNode(String nodeId, boolean includeConfigChannel);
    
    public void markAllConfigAsSentForNode(String nodeId);
    
    public void updateAbandonedRoutingBatches();

    public OutgoingBatch findOutgoingBatch(long batchId, String nodeId);

    public OutgoingBatches getOutgoingBatches(String nodeId, boolean includeDisabledChannels);

    public OutgoingBatches getOutgoingBatches(String nodeId, String channelId, boolean includeDisabledChannels);

    public OutgoingBatches getOutgoingBatches(String nodeId, String channelThread, NodeGroupLinkAction eventAction, 
            NodeGroupLinkAction defaultEventAction, boolean includeDisabledChannels);

    public OutgoingBatches getOutgoingBatchRange(long startBatchId, long endBatchId);
    
    public OutgoingBatches getOutgoingBatchByLoad(long loadI);
    
    public OutgoingBatches getOutgoingBatchByLoadRangeAndTable(long loadId, long startBatchId, 
            long endBatchId, String tableName);
   
    public int cancelLoadBatches(long loadId);
    
    public OutgoingBatches getOutgoingBatchRange(String nodeId, Date startDate, Date endDate, String... channels);

    public OutgoingBatches getOutgoingBatchErrors(int maxRows);
    
    public boolean isInitialLoadComplete(String nodeId);
    
    public boolean areAllLoadBatchesComplete(String nodeId);

    public boolean isUnsentDataOnChannelForNode(String channelId, String nodeId);

    public void updateOutgoingBatch(OutgoingBatch batch);
    
    public void updateOutgoingBatchStatus(ISqlTransaction transaction, Status status, String nodeId, long startBatchId, long endBatchId);

    public void updateCommonBatchExtractStatistics(OutgoingBatch batch);
    
    public void updateOutgoingBatch(ISqlTransaction transaction, OutgoingBatch outgoingBatch);

    public void updateOutgoingBatches(List<OutgoingBatch> batches);

    public void insertOutgoingBatch(OutgoingBatch outgoingBatch);
    
    public void insertOutgoingBatch(ISqlTransaction transaction, OutgoingBatch outgoingBatch);

    public int countOutgoingBatchesInError();
    
    public int countOutgoingBatchesUnsent();
    
    public int countOutgoingBatchesInError(String channelId);
    
    public int countOutgoingBatchesUnsent(String channelId);

    public int countOutgoingBatchesUnsentHeartbeat();

    public Map<String, Integer> countOutgoingBatchesPendingByChannel(String nodeId);
    
    public long countUnsentRowsByTargetNode(String nodeId);
    
    public int countUnsentBatchesByTargetNode(String nodeId);
    
    public List<OutgoingBatchSummary> findOutgoingBatchSummary(OutgoingBatch.Status ... statuses);
    
    public List<OutgoingBatchSummary> findOutgoingBatchSummaryByChannel(OutgoingBatch.Status ... statuses);    
    
    public List<OutgoingBatchSummary> findOutgoingBatchSummaryByNode(String nodeId,
    		Date sinceCreateTime, Status... statuses);
    
    public List<OutgoingBatchSummary> findOutgoingBatchSummaryByNodeAndChannel(String nodeId, String channelId,
    		Date sinceCreateTime, Status... statuses);
    
    public int countOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses, List<Long> loads);
    
    public List<OutgoingBatch> listOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses, List<Long> loads, long startAtBatchId, int rowsExpected, boolean ascending);
    
    public List<OutgoingLoadSummary> getLoadSummaries(boolean activeOnly);

    public Map<String, LoadCounts> getActiveLoadCounts();
    
    public List<LoadSummary> getQueuedLoads(String sourceNodeId);
    
    public LoadSummary getLoadSummary(long loadId);
    
    public Map<String, Integer> getLoadOverview(long loadId);
    
    public Collection<LoadSummary> getLoadHistory(String sourceNodeId, final String symTablePrefix, int rowsReturned);
    
    public Map<String, Map<String, LoadStatusSummary>> getLoadStatusSummaries(int loadId);
    
    public void copyOutgoingBatches(String channelId, long startBatchId, String fromNodeId, String toNodeId);
    
    public List<Long> getAllBatches();

}