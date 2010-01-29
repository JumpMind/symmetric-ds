/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
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

import org.jumpmind.symmetric.util.AppUtils;

public class OutgoingBatchHistory implements Serializable {

    private static final long serialVersionUID = 1L;

    private static String thisHostName;

    public enum Status {
        OK, ER, SK, SE, NE;
    }

    private long batchId;

    private String nodeId;

    private Status status;

    private Date startTime;

    private Date endTime;

    private long networkMillis;

    private long filterMillis;

    private long databaseMillis;

    private String hostName;

    private long byteCount;

    private long dataEventCount;

    private long failedDataId;

    private String sqlState;

    private int sqlCode;

    private String sqlMessage;

    static {
        thisHostName = AppUtils.getServerId();
    }

    public OutgoingBatchHistory() {
        this.hostName = thisHostName;
        this.status = Status.NE;
        this.startTime = new Date();
    }

    public OutgoingBatchHistory(OutgoingBatch batch) {
        this();
        this.batchId = batch.getBatchId();
        this.nodeId = batch.getNodeId();
    }

    public OutgoingBatchHistory(BatchInfo batch) {
        this();
        this.batchId = new Long(batch.getBatchId());
        this.nodeId = batch.getNodeId();
        this.status = batch.isOk() ? Status.OK : Status.ER;
        this.networkMillis = batch.getNetworkMillis();
        this.filterMillis = batch.getFilterMillis();
        this.databaseMillis = batch.getDatabaseMillis();
        this.byteCount = batch.getByteCount();
        this.sqlState = batch.getSqlState();
        this.sqlCode = batch.getSqlCode();
        this.sqlMessage = batch.getSqlMessage();
    }

    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public long getDataEventCount() {
        return dataEventCount;
    }

    public void setDataEventCount(long dataEventCount) {
        this.dataEventCount = dataEventCount;
    }

    public long getFailedDataId() {
        return failedDataId;
    }

    public void setFailedDataId(long failedDataId) {
        this.failedDataId = failedDataId;
    }

    public Status getStatus() {
        return status;
    }

    public void setStatus(Status status) {
        this.status = status;
    }

    public long getByteCount() {
        return byteCount;
    }

    public void setByteCount(long byteCount) {
        this.byteCount = byteCount;
    }

    public long getDatabaseMillis() {
        return databaseMillis;
    }

    public void setDatabaseMillis(long databaseMillis) {
        this.databaseMillis = databaseMillis;
    }

    public Date getEndTime() {
        return endTime;
    }

    public void setEndTime(Date endTime) {
        this.endTime = endTime;
    }

    public long getFilterMillis() {
        return filterMillis;
    }

    public void setFilterMillis(long filterMillis) {
        this.filterMillis = filterMillis;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
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

    public String getSqlState() {
        return sqlState;
    }

    public void setSqlState(String sqlState) {
        this.sqlState = sqlState;
    }

    public Date getStartTime() {
        return startTime;
    }

    public void setStartTime(Date startTime) {
        this.startTime = startTime;
    }

    public String getHostName() {
        return hostName;
    }

    public void setHostName(String hostName) {
        this.hostName = hostName;
    }

    public long getNetworkMillis() {
        return networkMillis;
    }

    public void setNetworkMillis(long networkMillis) {
        this.networkMillis = networkMillis;
    }

}
