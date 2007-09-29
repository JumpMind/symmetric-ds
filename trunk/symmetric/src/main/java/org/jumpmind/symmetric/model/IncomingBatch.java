package org.jumpmind.symmetric.model;

import java.io.Serializable;

import org.jumpmind.symmetric.load.IDataLoaderContext;

public class IncomingBatch implements Serializable {

    private static final long serialVersionUID = 1L;

    public enum Status {
        OK, ER;
    }

    private String batchId;

    private String clientId;

    private Status status;

    private boolean isRetry;

    public IncomingBatch() {
    }

    public IncomingBatch(IDataLoaderContext context) {
        batchId = context.getBatchId();
        clientId = context.getClientId();
        status = Status.OK;
    }
    
    public String getClientBatchId() {
        return clientId + "-" + batchId;
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

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public boolean isRetry() {
        return isRetry;
    }

    public void setRetry(boolean isRetry) {
        this.isRetry = isRetry;
    }

}
