package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataProcessorListener {

    public boolean processTable(DataContext context, Batch batch, Table table);
    
    /**
     * @param batch
     * @return true if this batch should be processed
     */
    public boolean batchBegin(DataContext context, Batch batch);

    public void batchBeforeCommit(DataContext context, Batch batch);

    public void batchEarlyCommit(DataContext context, Batch batch, int rowNumber);

    public void batchCommit(DataContext context, Batch batch);

    public void batchRollback(DataContext context, Batch batch, Exception ex);

}
