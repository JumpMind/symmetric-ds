package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public class DataProcessor {

    private static final String STAT_WRITE_DATA = "statWriteData";
    private static final String STAT_READ_DATA = "statReadData";

    static final Log log = LogFactory.getLog(DataProcessor.class);

    protected IDataReader dataReader;
    protected IDataWriter dataWriter;
    protected IDataProcessorListener listener;
    protected IDataWriterErrorHandler errorHandler;

    public DataProcessor() {
    }

    public DataProcessor(IDataReader dataReader, IDataWriter dataWriter) {
        this(dataReader, dataWriter, null, null);
    }

    public DataProcessor(IDataReader dataReader, IDataWriter dataWriter,
            IDataProcessorListener listener, IDataWriterErrorHandler errorHandler) {
        this.dataReader = dataReader;
        this.dataWriter = dataWriter;
        this.listener = listener;
        this.errorHandler = errorHandler;
    }

    public void process() {
        process(new DataContext());
    }

    public void process(DataContext ctx) {
        try {
            dataReader.open(ctx);
            boolean dataWriterOpened = false;
            Batch batch = null;
            do {
                batch = dataReader.nextBatch();
                if (batch != null) {
                    int dataRow = 0;
                    boolean processBatch = listener == null ? true : listener
                            .batchBegin(ctx, batch);
                    if (processBatch) {
                        if (!dataWriterOpened) {
                            dataWriter.open(ctx);
                        }
                        dataWriter.startBatch(batch);
                    }
                    dataRow += forEachTableInBatch(ctx, processBatch, batch);
                    if (processBatch) {
                        if (listener != null) {
                            listener.batchBeforeCommit(ctx, batch);
                        }
                        dataWriter.finishBatch(batch);
                        if (listener != null) {
                            listener.batchCommit(ctx, batch);
                        }
                    }
                }
            } while (batch != null);
        } finally {
            close(this.dataReader);
            close(this.dataWriter);
        }
    }

    protected int forEachTableInBatch(DataContext ctx, boolean processBatch, Batch batch) {
        int dataRow = 0;
        Table table = null;
        do {
            table = dataReader.nextTable();
            if (table != null) {
                if (listener == null || listener.processTable(ctx, batch, table)) {
                    boolean processTable = false;
                    if (processBatch) {
                        processTable = dataWriter.writeTable(table);
                    }
                    dataRow += forEachDataInTable(ctx, processTable, batch);
                }
            }
        } while (table != null);
        return dataRow;
    }

    protected int forEachDataInTable(DataContext ctx, boolean processTable, Batch batch) {
        int dataRow = 0;
        Data data = null;
        do {
            batch.startTimer(STAT_READ_DATA);
            data = dataReader.nextData();
            batch.incrementDataReadMillis(batch.endTimer(STAT_READ_DATA));
            if (data != null) {
                try {
                    dataRow++;
                    if (processTable) {
                        batch.startTimer(STAT_WRITE_DATA);
                        batch.incrementLineCount();
                        boolean needsCommit = dataWriter.writeData(data);
                        batch.incrementDataWriteMillis(batch.endTimer(STAT_WRITE_DATA));
                        if (needsCommit && listener != null) {
                            listener.batchEarlyCommit(ctx, batch, dataRow);
                        }
                    }
                } catch (Exception ex) {
                    if (errorHandler != null) {
                        if (!errorHandler.handleWriteError(ex, batch, data, dataRow)) {
                            rethrow(ex);
                        }
                    } else {
                        rethrow(ex);
                    }
                }
            }
        } while (data != null);
        return dataRow;
    }

    protected void rethrow(Exception ex) {
        if (ex instanceof RuntimeException) {
            throw (RuntimeException) ex;
        } else {
            throw new RuntimeException(ex);
        }
    }

    protected void close(IDataResource dataResource) {
        try {
            dataResource.close();
        } catch (Exception ex) {
            log.error(ex);
        }
    }

    public void setErrorHandler(IDataWriterErrorHandler errorHandler) {
        this.errorHandler = errorHandler;
    }

    public void setListener(IDataProcessorListener listener) {
        this.listener = listener;
    }

}
