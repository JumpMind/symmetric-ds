package org.jumpmind.symmetric.data.process;

import org.jumpmind.symmetric.data.model.Batch;
import org.jumpmind.symmetric.data.model.Data;
import org.jumpmind.symmetric.data.model.Table;

public class DataProcessor {

    protected IDataReader<DataContext> dataReader;
    protected IDataWriter<DataContext> dataWriter;
    protected IDataProcessorListener listener;
    protected IDataWriterErrorHandler errorHandler;

    public void process() {
        DataContext readerContext = dataReader.createDataContext();
        DataContext writerContext = dataWriter.createDataContext();
        dataReader.open(readerContext);
        Batch batch = null;
        do {
            batch = dataReader.nextBatch(readerContext);
            if (batch != null) {
                int dataRow = 0;
                readerContext.setBatch(batch);
                boolean processBatch = listener.batchBegin(batch);
                if (processBatch) {
                    writerContext.setBatch(batch);
                    dataWriter.startBatch(writerContext);
                }
                dataRow += forEachTableInBatch(processBatch, batch, readerContext, writerContext);
                if (processBatch) {
                    listener.batchBeforeCommit(batch);
                    dataWriter.finishBatch(writerContext);
                    listener.batchCommit(batch);
                }
            }
        } while (batch != null);
    }

    protected int forEachTableInBatch(boolean processBatch, Batch batch, DataContext readerContext,
            DataContext writerContext) {
        int dataRow = 0;
        Table table = null;
        do {
            table = dataReader.nextTable(readerContext);
            if (table != null) {
                readerContext.setSourceTable(table);
                if (processBatch) {
                    writerContext.setSourceTable(table);
                    dataWriter.switchTables(writerContext);
                }
                dataRow += forEachDataInTable(processBatch, batch, readerContext, writerContext);
            }
        } while (table != null);
        return dataRow;
    }

    protected int forEachDataInTable(boolean processBatch, Batch batch, DataContext readerContext,
            DataContext writerContext) {
        int dataRow = 0;
        Data data = null;
        do {
            data = dataReader.nextData(readerContext);
            if (data != null) {
                try {
                    dataRow++;
                    if (processBatch) {
                        dataWriter.writeData(data, writerContext);
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
