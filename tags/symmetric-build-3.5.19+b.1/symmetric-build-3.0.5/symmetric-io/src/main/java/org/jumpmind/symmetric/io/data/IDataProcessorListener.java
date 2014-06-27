package org.jumpmind.symmetric.io.data;


public interface IDataProcessorListener {

    /**
     * @return true if this batch should be processed
     */
    public boolean beforeBatchStarted(DataContext context);
    
    public void afterBatchStarted(DataContext context);

    public void beforeBatchEnd(DataContext context);

    public void batchSuccessful(DataContext context);

    public void batchInError(DataContext context, Exception ex);

}
