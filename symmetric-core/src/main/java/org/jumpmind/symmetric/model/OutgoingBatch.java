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
public class OutgoingBatch extends AbstractBatch {

    private static final long serialVersionUID = 1L;

    public enum Status {
        OK("Ok"), ER("Error"), RQ("Request"), NE("New"), QY("Querying"), SE("Sending"), LD("Loading"), RT("Routing"), IG("Ignored"), 
            RS("Resend"), XX("Unknown");

        private String description;

        Status(String description) {
            this.description = description;
        }

        @Override
        public String toString() {
            return description;
        }
    }
    
    private long loadId = -1;

    private Status status = Status.RT;

    private boolean loadFlag;
    
    private boolean extractJobFlag;
    
    private boolean commonFlag;

    private long extractMillis;

    private long sentCount;

    private long extractCount;

    private long loadCount;
    
    private long dataEventCount;

    private long reloadEventCount;

    private long insertEventCount;

    private long updateEventCount;

    private long deleteEventCount;

    private long otherEventCount;

    private long failedDataId;
    
    private long oldDataEventCount = 0;
    private long oldByteCount = 0;
    private long oldFilterMillis = 0;
    private long oldExtractMillis = 0;
    private long oldLoadMillis = 0;
    private long oldNetworkMillis = 0;
    
    public OutgoingBatch() {
    }

    public OutgoingBatch(String nodeId, String channelId, Status status) {
        setNodeId(nodeId);
        setChannelId(channelId);
        this.status = status;
        setCreateTime(new Date());
    }

    public void resetStats() {
        // save off old stats in case there
        // is an error and we want to be able to
        // restore the previous stats
        this.oldExtractMillis = this.extractMillis;
        this.oldDataEventCount = this.dataEventCount;
        this.oldByteCount = getByteCount();
        this.oldNetworkMillis = getNetworkMillis();
        this.oldFilterMillis = getFilterMillis();
        this.oldLoadMillis = getLoadMillis();

        this.extractMillis = 0;
        this.dataEventCount = 0;
        setByteCount(0);
        setNetworkMillis(0);
        setFilterMillis(0);
        setLoadMillis(0);
    }

    public void revertStatsOnError() {
        if (this.oldDataEventCount > 0) {
            this.extractMillis = this.oldExtractMillis;
            this.dataEventCount = this.oldDataEventCount;
            setByteCount(this.oldByteCount);
            setNetworkMillis(this.oldNetworkMillis);
            setFilterMillis(this.oldFilterMillis);
            setLoadMillis(this.oldLoadMillis);
        }
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

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public void setStatus(String status) {
        try {
            this.status = Status.valueOf(status);
        } catch (IllegalArgumentException e) {
            this.status = Status.XX;
        }
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

    public long getFailedDataId() {
        return failedDataId;
    }

    public void setFailedDataId(long failedDataId) {
        this.failedDataId = failedDataId;
    }
    
    @Override
    public Date getLastUpdatedTime() {
        if (super.getLastUpdatedTime() == null) {
            return new Date();
        } else {
            return super.getLastUpdatedTime();
        }
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
    
    @Override
    public String getStagedLocation() {
        return Batch.getStagedLocation(commonFlag, getNodeId());
    }
    
    @Override
    public String toString() {
        return getNodeBatchId();
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