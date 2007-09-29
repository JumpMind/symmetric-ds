package org.jumpmind.symmetric.service;

import java.util.List;

import org.jumpmind.symmetric.model.IncomingBatch;
import org.jumpmind.symmetric.model.IncomingBatchHistory;
import org.springframework.transaction.annotation.Transactional;

public interface IIncomingBatchService {

    public IncomingBatch findIncomingBatch(String batchId, String clientId);

    public List<IncomingBatchHistory> findIncomingBatchHistory(String batchId, String clientId);

    @Transactional
    public boolean acquireIncomingBatch(IncomingBatch status);
    
    @Transactional
    public void insertIncomingBatch(IncomingBatch status);
    
    @Transactional
    public void insertIncomingBatchHistory(IncomingBatchHistory history);
    
    @Transactional
    public int updateIncomingBatch(IncomingBatch status);
}
