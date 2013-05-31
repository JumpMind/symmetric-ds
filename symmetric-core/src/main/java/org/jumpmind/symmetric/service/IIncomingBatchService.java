
package org.jumpmind.symmetric.service;

import java.util.Date;
import java.util.List;

import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.model.IncomingBatch;

/**
 * This service provides an API to access to the incoming batch table. 
 */
public interface IIncomingBatchService {

    public int countIncomingBatchesInError();
    
    public int countIncomingBatchesInError(String channelId);
    
    public IncomingBatch findIncomingBatch(long batchId, String nodeId);

    public List<IncomingBatch> findIncomingBatchErrors(int maxRows);

    public boolean acquireIncomingBatch(IncomingBatch batch);

    public void insertIncomingBatch(IncomingBatch batch);
    
    public int updateIncomingBatch(ISqlTransaction transaction, IncomingBatch batch);

    public int updateIncomingBatch(IncomingBatch batch);
    
    public int deleteIncomingBatch(IncomingBatch batch);
    
    public List<Date> listIncomingBatchTimes(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, boolean ascending);
    
    public List<IncomingBatch> listIncomingBatches(List<String> nodeIds, List<String> channels,
            List<IncomingBatch.Status> statuses, Date startAtCreateTime, int maxRowsToRetrieve, boolean ascending);

    public void markIncomingBatchesOk(String nodeId);
    
    public List<IncomingBatch> listIncomingBatchesInErrorFor(String nodeId);

}