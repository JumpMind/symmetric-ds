package org.jumpmind.symmetric.model;

public class DataEvent {

    private long dataId;

    private String nodeId;
    
    private Long batchId;

    public DataEvent() {
    }
    
    public DataEvent(long dataId, String nodeId) {
        this.dataId = dataId;
        this.nodeId = nodeId;
    }
    
    public Long getBatchId() {
        return batchId;
    }

    public void setBatchId(Long batchId) {
        this.batchId = batchId;
    }

    public long getDataId() {
        return dataId;
    }

    public void setDataId(long dataId) {
        this.dataId = dataId;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

}
