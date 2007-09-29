package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.OutgoingBatch;
import org.jumpmind.symmetric.model.OutgoingBatch.Status;

public interface IOutgoingBatchService {
    public void insertOutgoingBatch(final OutgoingBatch outgoingBatch);
    public void buildOutgoingBatches(String clientId);
    public List<OutgoingBatch> getOutgoingBatches(String clientId);
    public void markOutgoingBatchSent(OutgoingBatch batch);
    public void setBatchStatus(String batchId, Status status);
}
