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

import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.DataEventType;

/**
 * Used for tracking the sending a collection of data to a node in the system. A
 * new outgoing_batch is created and given a status of 'NE'. After sending the
 * outgoing_batch to its target node, the status becomes 'SE'. The node responds
 * with either a success status of 'OK' or an error status of 'ER'. An error
 * while sending to the node also results in an error status of 'ER' regardless
 * of whether the node sends that acknowledgement.
 */
public class OutgoingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        OK("Ok"), ER("Error"), RQ("Request"), NE("New"), QY("Querying"), SE("Sending"), LD("Loading"), RT("Routing"), IG("Ignored"), XX("Unknown");

        private String description;

        Status(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }

    private long batchId = -1;

    private String nodeId;

    private String channelId;
    
    private long loadId = -1;

    private Status status = Status.RT;

    private boolean loadFlag;

    private boolean errorFlag;
    
    private boolean extractJobFlag;
    
    private boolean commonFlag;

    private long routerMillis;

    private long networkMillis;

    private long filterMillis;

    private long loadMillis;

    private long extractMillis;

    private long byteCount;

    private long sentCount;

    private long extractCount;

    private long loadCount;
    
    private long ignoreCount;

    private long dataEventCount;

    private long reloadEventCount;

    private long insertEventCount;

    private long updateEventCount;

    private long deleteEventCount;

    private long otherEventCount;

    private long failedDataId;

    private String sqlState;

    private int sqlCode;

    private String sqlMessage;

    private String lastUpdatedHostName;

    private Date lastUpdatedTime;

    private Date createTime;
    
    private String createBy;

    private long oldDataEventCount = 0;
    private long oldByteCount = 0;
    private long oldFilterMillis = 0;
    private long oldExtractMillis = 0;
    private long oldLoadMillis = 0;
    private long oldNetworkMillis = 0;

    public OutgoingBatch() {
    }

    public OutgoingBatch(String nodeId, String channelId, Status status) {
        this.nodeId = nodeId;
        this.channelId = channelId;
        this.status = status;
        this.createTime = new Date();
    }

    public void resetStats() {
        // save off old stats in case there
        // is an error and we want to be able to
        // restore the previous stats
        this.oldByteCount = this.byteCount;
        this.oldDataEventCount = this.dataEventCount;
        this.oldExtractMillis = this.extractMillis;
        this.oldLoadMillis = this.loadMillis;
        this.oldNetworkMillis = this.networkMillis;
        this.oldFilterMillis = this.filterMillis;

        this.dataEventCount = 0;
        this.byteCount = 0;
        this.filterMillis = 0;
        this.extractMillis = 0;
        this.loadMillis = 0;
        this.networkMillis = 0;
    }

    public void revertStatsOnError() {
        if (this.oldDataEventCount > 0) {
            this.byteCount = this.oldByteCount;
            this.dataEventCount = this.oldDataEventCount;
            this.extractMillis = this.oldExtractMillis;
            this.loadMillis = this.oldLoadMillis;
            this.networkMillis = this.oldNetworkMillis;
            this.filterMillis = this.oldFilterMillis;
        }
    }

    public void setErrorFlag(boolean errorFlag) {
        this.errorFlag = errorFlag;
    }

    public boolean isErrorFlag() {
        return errorFlag;
    }

    public void setLoadFlag(boolean loadFlag) {
        this.loadFlag = loadFlag;
    }

    public boolean isLoadFlag() {
        return loadFlag;
    }

    public void setSentCount(long sentCount) {
        this.sentCount = sentCount;
    }

    public long getSentCount() {
        return sentCount;
    }

    public void setExtractCount(long extractCount) {
        this.extractCount = extractCount;
    }

    public long getExtractCount() {
        return extractCount;
    }

    public void setLoadCount(long loadCount) {
        this.loadCount = loadCount;
    }

    public long getLoadCount() {
        return loadCount;
    }

    public String getNodeBatchId() {
        return nodeId + "-" + batchId;
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String locationId) {
        this.nodeId = locationId;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public void setStatus(String status) {
        try {
            this.status = Status.valueOf(status);
        } catch (IllegalArgumentException e) {
            this.status = Status.XX;
        }
    }

    public long getRouterMillis() {
        return routerMillis;
    }

    public void setUpdateEventCount(long updateEventCount) {
        this.updateEventCount = updateEventCount;
    }

    public long getUpdateEventCount() {
        return updateEventCount;
    }

    public void setDeleteEventCount(long deleteEventCount) {
        this.deleteEventCount = deleteEventCount;
    }

    public long getDeleteEventCount() {
        return deleteEventCount;
    }

    public void incrementEventCount(DataEventType type) {
        switch (type) {
        case RELOAD:
            reloadEventCount++;
            break;
        case INSERT:
            insertEventCount++;
            break;
        case UPDATE:
            updateEventCount++;
            break;
        case DELETE:
            deleteEventCount++;
            break;
        default:
            otherEventCount++;
            break;
        }
    }

    public void setInsertEventCount(long insertEventCount) {
        this.insertEventCount = insertEventCount;
    }

    public long getInsertEventCount() {
        return insertEventCount;
    }

    public void setOtherEventCount(long otherEventCount) {
        this.otherEventCount = otherEventCount;
    }

    public long getOtherEventCount() {
        return otherEventCount;
    }

    public void setReloadEventCount(long reloadEventCount) {
        this.reloadEventCount = reloadEventCount;
    }

    public long getReloadEventCount() {
        return reloadEventCount;
    }

    public void setRouterMillis(long routerMillis) {
        this.routerMillis = routerMillis;
    }

    public long getNetworkMillis() {
        return networkMillis;
    }

    public void setNetworkMillis(long networkMillis) {
        this.networkMillis = networkMillis;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public long getLoadMillis() {
        return loadMillis;
    }

    public void setLoadMillis(long databaseMillis) {
        this.loadMillis = databaseMillis;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public long getDataEventCount() {
        return dataEventCount;
    }

    public void setDataEventCount(long dataEventCount) {
        this.dataEventCount = dataEventCount;
    }

    public void setExtractMillis(long extractMillis) {
        this.extractMillis = extractMillis;
    }

    public long getExtractMillis() {
        return extractMillis;
    }

    public void incrementDataEventCount() {
        this.dataEventCount++;
    }
    
    public void incrementInsertEventCount() {
        this.insertEventCount++;
    }

    public void incrementByteCount(int size) {
        this.byteCount += size;
    }

    public long getFailedDataId() {
        return failedDataId;
    }

    public void setFailedDataId(long failedDataId) {
        this.failedDataId = failedDataId;
    }

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public int getSqlCode() {
        return sqlCode;
    }

    public void setSqlCode(int sqlCode) {
        this.sqlCode = sqlCode;
    }

    public String getSqlMessage() {
        return sqlMessage;
    }

    public void setSqlMessage(String sqlMessage) {
        this.sqlMessage = sqlMessage;
    }

    public String getLastUpdatedHostName() {
        return lastUpdatedHostName;
    }

    public void setLastUpdatedHostName(String lastUpdatedHostName) {
        this.lastUpdatedHostName = lastUpdatedHostName;
    }

    public Date getLastUpdatedTime() {
        if (lastUpdatedTime == null) {
            return new Date();
        } else {
            return lastUpdatedTime;
        }
    }

    public void setLastUpdatedTime(Date lastUpdatedTime) {
        this.lastUpdatedTime = lastUpdatedTime;
    }

    public Date getCreateTime() {
        return createTime;
    }

    public void setCreateTime(Date createTime) {
        this.createTime = createTime;
    }
    
    public void setIgnoreCount(long ignoreCount) {
        this.ignoreCount = ignoreCount;
    }
    
    public long getIgnoreCount() {
        return ignoreCount;
    }
    
    public void incrementIgnoreCount() {
        ignoreCount++;
    }

    public long totalEventCount() {
        return insertEventCount + updateEventCount + deleteEventCount + otherEventCount;
    }
    
    public void setCommonFlag(boolean commonFlag) {
        this.commonFlag = commonFlag;
    }
    
    public boolean isCommonFlag() {
        return commonFlag;
    }
    
    public String getStagedLocation() {
        return Batch.getStagedLocation(commonFlag, nodeId);
    }
    
    @Override
    public String toString() {
        return getNodeBatchId();
    }
    
    public void setCreateBy(String createBy) {
        this.createBy = createBy;
    }
    
    public String getCreateBy() {
        return createBy;
    }
    
    public void setLoadId(long loadId) {
        this.loadId = loadId;
    }

    public long getLoadId() {
        return loadId;
    }
    
    public void setExtractJobFlag(boolean extractJobFlag) {
        this.extractJobFlag = extractJobFlag;
    }
    
    public boolean isExtractJobFlag() {
        return extractJobFlag;
    }

}