package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IMissingTableHandler;
import org.jumpmind.symmetric.model.IncomingBatch;

abstract public class DataLoaderFilterAdapter implements IDataLoaderFilter, IBatchListener, IMissingTableHandler {

    protected boolean autoRegister;

    public DataLoaderFilterAdapter(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public boolean isAutoRegister() {
        return autoRegister;
    }

    public void earlyCommit(IDataLoader loader, IncomingBatch batch) {

    }

    public void batchComplete(IDataLoader loader, IncomingBatch batch) {

    }

    public void batchCommitted(IDataLoader loader, IncomingBatch batch) {

    }

    public void batchRolledback(IDataLoader loader, IncomingBatch batch, Exception ex) {

    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        return true;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues,
            String[] keyValues) {

        return true;
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {

        return true;
    }
    
    public boolean isHandlingMissingTable(IDataLoaderContext ctx) {
        return false;
    }

}
