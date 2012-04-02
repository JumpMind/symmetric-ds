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
import java.util.Date;

/**
 * Holder class for summary information about incoming batches
 */
public class IncomingBatchSummary implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String nodeId;
    private int batchCount;
    private int dataCount;
    private IncomingBatch.Status status;
    private Date oldestBatchCreateTime;

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    public int getBatchCount() {
        return batchCount;
    }

    public void setBatchCount(int batchCount) {
        this.batchCount = batchCount;
    }

    public int getDataCount() {
        return dataCount;
    }

    public void setDataCount(int dataCount) {
        this.dataCount = dataCount;
    }

    public IncomingBatch.Status getStatus() {
        return status;
    }

    public void setStatus(IncomingBatch.Status status) {
        this.status = status;
    }

    public Date getOldestBatchCreateTime() {
        return oldestBatchCreateTime;
    }

    public void setOldestBatchCreateTime(Date oldestBatchCreateTime) {
        this.oldestBatchCreateTime = oldestBatchCreateTime;
    }

}
