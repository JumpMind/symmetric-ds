package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.BatchInfo;

public interface IAcknowledgeService
{
    public void ack(List<BatchInfo> batches);
}
