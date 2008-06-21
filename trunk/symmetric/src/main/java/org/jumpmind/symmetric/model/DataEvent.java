package org.jumpmind.symmetric.model;

public class DataEvent {

    private long dataId;

    private String nodeId;

    private Long batchId;

    private boolean batched;

    private String channelId;

    private String transactionId;

    public DataEvent() {
    }

    public DataEvent(long dataId, String nodeId, String channelId) {
        this.dataId = dataId;
        this.nodeId = nodeId;
        this.channelId = channelId;
    }

    public DataEvent(long dataId, String nodeId, String channelId, String transactionId) {
        this.dataId = dataId;
        this.nodeId = nodeId;
        this.channelId = channelId;
        this.transactionId = transactionId;
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

    public boolean isBatched() {
        return batched;
    }

    public void setBatched(boolean batched) {
        this.batched = batched;
    }

    public String getChannelId() {
        return channelId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }

    public String getTransactionId() {
        return transactionId;
    }

    public void setTransactionId(String transactionId) {
        this.transactionId = transactionId;
    }

}
