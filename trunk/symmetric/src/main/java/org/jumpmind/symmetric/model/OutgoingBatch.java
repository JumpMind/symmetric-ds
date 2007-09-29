package org.jumpmind.symmetric.model;

import java.io.Serializable;

public class OutgoingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        NE, SE, ER, OK;
    }

    private String batchId;

    private String clientId;

    private String channelId;

    private Status status;

    private BatchType batchType = BatchType.EVENTS;

    public OutgoingBatch() {
    }
    
    public OutgoingBatch(Node client, String channelId, BatchType batchType) {
        this.clientId = client.getNodeId();
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

    public String getClientId() {
        return clientId;
    }

    public void setClientId(String locationId) {
        this.clientId = locationId;
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
