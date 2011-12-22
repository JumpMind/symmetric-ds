package org.jumpmind.symmetric.io.data;


public interface IDataProcessorListener<R extends IDataReader, W extends IDataWriter> {

    /**
     * @return true if this batch should be processed
     */
    public boolean beforeBatchStarted(DataContext<R, W> context);
    
    public void afterBatchStarted(DataContext<R, W> context);

    public void beforeBatchEnd(DataContext<R, W> context);

    public void batchSuccessful(DataContext<R, W> context);

    public void batchInError(DataContext<R, W> context, Exception ex);

}
