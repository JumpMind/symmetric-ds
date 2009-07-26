package org.jumpmind.symmetric.model;

public class DataEvent {

    private long dataId;
   
    private long batchId;

    public DataEvent() {
    }

    public DataEvent(long dataId, long batchId) {
        this.dataId = dataId;
        this.batchId = batchId;
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

}
