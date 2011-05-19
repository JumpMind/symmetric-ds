package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public class DataProcessor<T extends DataContext> {

    protected IDataReader<T> dataReader;
    protected IDataWriter<T> dataWriter;
    protected IDataProcessorListener listener;
    protected IDataWriterErrorHandler errorHandler;

    public DataProcessor() {
    }

    public DataProcessor(IDataReader<T> dataReader, IDataWriter<T> dataWriter) {
        this(dataReader, dataWriter, null, null);
    }

    public DataProcessor(IDataReader<T> dataReader, IDataWriter<T> dataWriter,
            IDataProcessorListener listener, IDataWriterErrorHandler errorHandler) {
        this.dataReader = dataReader;
        this.dataWriter = dataWriter;
        this.listener = listener;
        this.errorHandler = errorHandler;
    }

    public void process() {
        T readerContext = dataReader.createDataContext();
        T writerContext = dataWriter.createDataContext();
        dataReader.open(readerContext);
        boolean dataWriterOpened = false;
        Batch batch = null;
        do {
            batch = dataReader.nextBatch(readerContext);
            if (batch != null) {
                int dataRow = 0;
                boolean processBatch = listener == null ? true : listener.batchBegin(batch);
                if (processBatch) {
                    if (!dataWriterOpened) {
                        writerContext.setBinaryEncoding(readerContext.getBinaryEncoding());
                        dataWriter.open(writerContext);
                    }
                    dataWriter.startBatch(batch);
                }
                dataRow += forEachTableInBatch(processBatch, batch, readerContext, writerContext);
                if (processBatch) {
                    if (listener != null) {
                        listener.batchBeforeCommit(batch);
                    }
                    dataWriter.finishBatch(batch);
                    if (listener != null) {
                        listener.batchCommit(batch);
                    }
                }
            }
        } while (batch != null);
    }

    protected int forEachTableInBatch(boolean processBatch, Batch batch, T readerContext,
            T writerContext) {
        int dataRow = 0;
        Table table = null;
        do {
            table = dataReader.nextTable(readerContext);
            if (table != null) {
                if (processBatch) {
                    processBatch |= dataWriter.switchTables(table);
                }
                dataRow += forEachDataInTable(processBatch, batch, readerContext, writerContext);
            }
        } while (table != null);
        return dataRow;
    }

    protected int forEachDataInTable(boolean processBatch, Batch batch, T readerContext,
            T writerContext) {
        int dataRow = 0;
        Data data = null;
        do {
            data = dataReader.nextData(readerContext);
            if (data != null) {
                try {
                    dataRow++;
                    if (processBatch) {
                        dataWriter.writeData(data);
                    }
                } catch (Exception ex) {
                    if (errorHandler != null) {
                        if (!errorHandler.handleWriteError(ex, batch, data, dataRow)) {
                            rethrow(ex);
                        }
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

}
