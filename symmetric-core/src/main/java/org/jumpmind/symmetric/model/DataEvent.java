package org.jumpmind.symmetric.model;

import java.io.Serializable;

public class DataEvent implements Serializable {
    
    private static final long serialVersionUID = 1L;

    private long dataId;
   
    private long batchId;
    
    private String routerId;

    public DataEvent() {
    }

    public DataEvent(long dataId, long batchId, String routerId) {
        this.dataId = dataId;
        this.batchId = batchId;
        this.routerId = routerId;
    }
    
    public long getBatchId() {
        return batchId;
    }

    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public long getDataId() {
        return dataId;
    }

    public void setDataId(long dataId) {
        this.dataId = dataId;
    }

    public void setRouterId(String routerId) {
        this.routerId = routerId;
    }
    
    public String getRouterId() {
        return routerId;
    }
}