package org.jumpmind.symmetric.model;

import java.io.Serializable;
import java.util.List;

import org.jumpmind.symmetric.io.data.writer.StructureDataWriter.PayloadType;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;

public class OutgoingBatchWithPayload implements Serializable {

    private static final long serialVersionUID = 1L;

    private List<String> payload;

    private PayloadType payloadType;

    private Status status;

    private long batchId;
    
    private String channelId;

    public OutgoingBatchWithPayload(OutgoingBatch batch, PayloadType payloadType) {
        this.status = batch.getStatus();
        this.batchId = batch.getBatchId();
        this.channelId = batch.getChannelId();
        this.payloadType = payloadType;        
    }

    public PayloadType getPayloadType() {
        return payloadType;
    }

    public void setPayloadType(PayloadType payloadType) {
        this.payloadType = payloadType;
    }

    public List<String> getPayload() {
        return payload;
    }

    public void setPayload(List<String> payload) {
        this.payload = payload;
    }
    
    public Status getStatus() {
        return status;
    }
    
    public void setStatus(Status status) {
        this.status = status;
    }
    
    public long getBatchId() {
        return batchId;
    }
    
    public void setBatchId(long batchId) {
        this.batchId = batchId;
    }

    public void setChannelId(String channelId) {
        this.channelId = channelId;
    }
    
    public String getChannelId() {
        return channelId;
    }
}
