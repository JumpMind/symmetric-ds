package org.jumpmind.symmetric.service;


public interface IOutgoingBatchHistoryService
{
    public void created(int batchId, int eventCount);
    public void sent(int batchId);
    public void error(int batchId, long failedDataId);
    public void ok(int batchId);
}
