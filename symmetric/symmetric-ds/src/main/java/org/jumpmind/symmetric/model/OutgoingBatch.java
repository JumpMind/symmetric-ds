/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.Date;

public class OutgoingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        NE, QY, SE, LD, ER, OK, IG;
    }

    private long batchId;

    private String nodeId;

    private String channelId;

    private Status status = Status.NE;
    
    private boolean loadFlag;
    
    private boolean errorFlag;

    private long routerMillis;

    private long networkMillis;

    private long filterMillis;

    private long loadMillis;
    
    private long extractMillis;

    private long byteCount;

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

    private String sqlState;

    private int sqlCode;

    private String sqlMessage;

    private String lastUpdatedHostName;

    private Date lastUpdatedTime;

    private Date createTime;

    public OutgoingBatch() {
    }

    public OutgoingBatch(String nodeId, String channelId) {
        this.nodeId = nodeId;
        this.channelId = channelId;
        this.status = Status.NE;
        this.createTime = new Date();
    }
    
    public void resetStats() {
        this.dataEventCount = 0;
        this.byteCount = 0;
        this.filterMillis = 0;
        this.extractMillis = 0;
        this.loadMillis = 0;
        this.networkMillis = 0;
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
        this.status = Status.valueOf(status);
    }

    public BatchInfo getBatchInfo() {
        return new BatchInfo(this.batchId);
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

}
