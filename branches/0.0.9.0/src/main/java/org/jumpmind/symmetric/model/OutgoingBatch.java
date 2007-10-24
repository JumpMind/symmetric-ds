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
import java.util.ArrayList;
import java.util.List;

public class OutgoingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        NE, SE, ER, OK;
    }

    private String batchId;

    private String nodeId;

    private String channelId;

    private Status status = Status.NE;

    private BatchType batchType = BatchType.EVENTS;

    public OutgoingBatch() {
    }
    
    public OutgoingBatch(Node client, String channelId, BatchType batchType) {
        this.nodeId = client.getNodeId();
        this.channelId = channelId;
        this.status = Status.NE;
        this.batchType = batchType;
    }
    

    public String getBatchId() {
        return batchId;
    }

    public void setBatchId(String batchId) {
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

    public BatchType getBatchType() {
        return batchType;
    }
    
    public List<BatchInfo> getBatchInfoList() {
        List<BatchInfo> list = new ArrayList<BatchInfo>();
        list.add(new BatchInfo(this.batchId));
        return list;
    }

    public void setBatchType(BatchType batchType) {
        this.batchType = batchType;
    }

    public void setBatchType(String batchType) {
        if (BatchType.EVENTS.getCode().equals(batchType)) {
            this.batchType = BatchType.EVENTS;
        } else if (BatchType.INITIAL_LOAD.getCode().equals(batchType)) {
            this.batchType = BatchType.INITIAL_LOAD;
        } else {
            batchType = null;
        }
    }

}
