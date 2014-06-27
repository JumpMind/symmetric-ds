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
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.List;

/**
 * Indicates the status of an attempt to transport data from or to a remove
 * node.
 */
public class RemoteNodeStatus implements Serializable {

    private static final long serialVersionUID = 1L;

    public static enum Status {
        OFFLINE, BUSY, NOT_AUTHORIZED, REGISTRATION_REQUIRED, SYNC_DISABLED, NO_DATA, DATA_PROCESSED, DATA_ERROR, UNKNOWN_ERROR
    };

    private String nodeId;
    private Status status;
    private long dataProcessed;
    private long batchesProcessed;

    public RemoteNodeStatus(String nodeId) {
        this.status = Status.NO_DATA;
        this.nodeId = nodeId;
    }
    
    public boolean failed() {
        return status != Status.NO_DATA && status != Status.DATA_PROCESSED;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getDataProcessed() {
        return dataProcessed;
    }

    public long getBatchesProcessed() {
        return batchesProcessed;
    }

    public void updateIncomingStatus(List<IncomingBatch> incomingBatches) {
        if (incomingBatches != null) {
            for (IncomingBatch incomingBatch : incomingBatches) {
                dataProcessed += incomingBatch.getStatementCount();
                batchesProcessed++;
                if (incomingBatch.getStatus() == org.jumpmind.symmetric.model.IncomingBatch.Status.ER) {
                    status = Status.DATA_ERROR;
                }
            }
        }

        if (status != Status.DATA_ERROR && dataProcessed > 0) {
            status = Status.DATA_PROCESSED;
        }
    }

    public void updateOutgoingStatus(List<OutgoingBatch> outgoingBatches, List<BatchInfo> batches) {
        if (batches != null) {
            for (BatchInfo batch : batches) {
                if (!batch.isOk()) {
                    status = Status.DATA_ERROR;
                }
            }
        }

        if (outgoingBatches != null) {
            for (OutgoingBatch batch : outgoingBatches) {
                batchesProcessed++;
                dataProcessed += batch.totalEventCount();
            }
        }

        if (status != Status.DATA_ERROR && dataProcessed > 0) {
            status = Status.DATA_PROCESSED;
        }
    }

}
