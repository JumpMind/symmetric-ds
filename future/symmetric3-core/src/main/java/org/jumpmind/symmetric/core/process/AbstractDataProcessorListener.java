package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractDataProcessorListener implements IDataProcessorListener {

    public boolean processTable(DataContext context, Batch batch, Table table) {
        return true;
    }

    public boolean batchBegin(DataContext context, Batch batch) {

        return false;
    }

    public void batchBeforeCommit(DataContext context, Batch batch) {

        
    }

    public void batchEarlyCommit(DataContext context, Batch batch, int rowNumber) {

        
    }

    public void batchCommit(DataContext context, Batch batch) {

        
    }

    public void batchRollback(DataContext context, Batch batch, Exception ex) {

        
    }
    
    

}
