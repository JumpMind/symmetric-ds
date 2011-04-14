package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;

public interface IDataProcessorListener {

    /**
     * 
     * @param batch
     * @return true if this batch should be processed
     */
    public boolean batchBegin(Batch batch);

    public void batchBeforeCommit(Batch batch);

    public void batchEarlyCommit(Batch batch, int rowNumber);

    public void batchCommit(Batch batch);

    public void batchRollback(Batch batch, Exception ex);

}
