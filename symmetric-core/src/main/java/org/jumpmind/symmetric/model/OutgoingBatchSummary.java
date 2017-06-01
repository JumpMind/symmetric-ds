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
package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

/**
 * Holder class for summary information about outgoing batches
 */
public class OutgoingBatchSummary implements Serializable {

    private static final long serialVersionUID = 1L;
    
    private String nodeId;
    private int batchCount;
    private int dataCount;
    private OutgoingBatch.Status status;
    private Date oldestBatchCreateTime;
    private Date lastBatchUpdateTime;
    private String channel;
    private String sqlMessage;
    private long errorBatchId;
    private long totalBytes;
    private long totalMillis;
    
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

    public OutgoingBatch.Status getStatus() {
        return status;
    }

    public void setStatus(OutgoingBatch.Status status) {
        this.status = status;
    }

    public Date getOldestBatchCreateTime() {
        return oldestBatchCreateTime;
    }

    public void setOldestBatchCreateTime(Date oldestBatchCreateTime) {
        this.oldestBatchCreateTime = oldestBatchCreateTime;
    }

    public String getChannel() {
        return channel;
    }

    public void setChannel(String channel) {
        this.channel = channel;
    }

    public Date getLastBatchUpdateTime() {
        return lastBatchUpdateTime;
    }

    public void setLastBatchUpdateTime(Date lastBatchUpdateTime) {
        this.lastBatchUpdateTime = lastBatchUpdateTime;
    }

    public String getSqlMessage() {
        return sqlMessage;
    }

    public void setSqlMessage(String sqlMessage) {
        this.sqlMessage = sqlMessage;
    }

    public long getErrorBatchId() {
        return errorBatchId;
    }

    public void setErrorBatchId(long errorBatchId) {
        this.errorBatchId = errorBatchId;
    }

    public long getTotalBytes() {
        return totalBytes;
    }

    public void setTotalBytes(long totalBytes) {
        this.totalBytes = totalBytes;
    }

    public long getTotalMillis() {
        return totalMillis;
    }

    public void setTotalMillis(long totalMillis) {
        this.totalMillis = totalMillis;
    }

    
}
