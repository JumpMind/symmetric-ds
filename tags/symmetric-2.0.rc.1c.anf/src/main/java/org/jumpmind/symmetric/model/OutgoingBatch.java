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
        NE, SE, ER, OK, IG;
    }

    private long batchId;

    private String nodeId;

    private String channelId;

    private Status status = Status.NE;

    private long routerMillis;

    private long networkMillis;

    private long filterMillis;

    private long loadMillis;
    
    private long extractMillis;

    private long byteCount;

    private long sentCount;

    private long dataEventCount;

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

    public void setSentCount(long sentCount) {
        this.sentCount = sentCount;
    }

    public long getSentCount() {
        return sentCount;
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
