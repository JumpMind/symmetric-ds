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
 * under the License.  */


package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatchSummary;
import org.jumpmind.symmetric.model.OutgoingBatches;

/**
 * This service provides an API to access to the outgoing batch table. 
 */
public interface IOutgoingBatchService {

    public void markAllAsSentForNode(Node node);

    public void updateAbandonedRoutingBatches();

    public OutgoingBatch findOutgoingBatch(long batchId);

    public OutgoingBatches getOutgoingBatches(Node node, boolean includeDisabledChannels);

    public OutgoingBatches getOutgoingBatchRange(String startBatchId, String endBatchId);

    public OutgoingBatches getOutgoingBatchErrors(int maxRows);

    public boolean isInitialLoadComplete(String nodeId);
    
    public boolean areAllLoadBatchesComplete(String nodeId);

    public boolean isUnsentDataOnChannelForNode(String channelId, String nodeId);

    public void updateOutgoingBatch(OutgoingBatch batch);

    public void updateOutgoingBatches(List<OutgoingBatch> batches);

    public void insertOutgoingBatch(OutgoingBatch outgoingBatch);

    public int countOutgoingBatchesInError();
    
    public List<OutgoingBatchSummary> findOutgoingBatchSummary(OutgoingBatch.Status ... statuses);
    
    public int countOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses);
    
    public List<OutgoingBatch> listOutgoingBatches(List<String> nodeIds, List<String> channels,
            List<OutgoingBatch.Status> statuses, long startAtBatchId, int rowsExpected);

}