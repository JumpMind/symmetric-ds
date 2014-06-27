package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.writer.IgnoreBatchException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcessor {

    private static final String STAT_WRITE_DATA = "statWriteData";
    private static final String STAT_READ_DATA = "statReadData";

    static final Logger log = LoggerFactory.getLogger(DataProcessor.class);

    protected IDataReader dataReader;

    protected IDataWriter defaultDataWriter;

    protected IDataProcessorListener listener;

    protected Table currentTable;

    protected CsvData currentData;

    protected Batch currentBatch;

    public DataProcessor() {
    }

    public DataProcessor(IDataReader dataReader, IDataWriter defaultDataWriter) {
        this(dataReader, defaultDataWriter, null);
    }

    public DataProcessor(IDataReader dataReader, IDataWriter defaultDataWriter,
            IDataProcessorListener listener) {
        this.dataReader = dataReader;
        this.defaultDataWriter = defaultDataWriter;
        this.listener = listener;
    }

    /**
     * This method may be overridden in order to choose different
     * {@link IDataWriter} based on the batch that is being written.
     * 
     * @param batch
     *            The batch that is about to be written
     * @return The data writer to use for the writing of the batch
     */
    protected IDataWriter chooseDataWriter(Batch batch) {
        return this.defaultDataWriter;
    }

    public void process(DataContext context) {
        try {
            context.setReader(dataReader);
            dataReader.open(context);
            do {
                currentBatch = dataReader.nextBatch();
                if (currentBatch != null) {
                    context.setBatch(currentBatch);
                    boolean endBatchCalled = false;
                    IDataWriter dataWriter = null;
                    try {
                        boolean processBatch = listener == null ? true : listener
                                .beforeBatchStarted(context);

                        if (processBatch) {
                            dataWriter = chooseDataWriter(currentBatch);
                            processBatch &= dataWriter != null;
                        }

                        if (processBatch) {
                            context.setWriter(dataWriter);
                            dataWriter.open(context);
                            dataWriter.start(currentBatch);
                            if (listener != null) {
                                listener.afterBatchStarted(context);
                            }
                        }

                        // pull and process any data events that are not wrapped
                        // in a table
                        forEachDataInTable(context, processBatch, currentBatch);

                        // pull and process all data events wrapped in tables
                        forEachTableInBatch(context, processBatch, currentBatch);

                        if (processBatch) {
                            if (listener != null) {
                                listener.beforeBatchEnd(context);
                            }
                            dataWriter.end(currentBatch, false);
                            endBatchCalled = true;
                            if (listener != null) {
                                listener.batchSuccessful(context);
                            }
                        }
                    } catch (Exception ex) {
                        try {
                            if (dataWriter != null && !endBatchCalled) {
                                dataWriter.end(currentBatch, true);
                            }
                        } finally {
                            if (listener != null) {
                                listener.batchInError(context, ex);
                            }
                        }
                        rethrow(ex);
                    } finally {
                        close(dataWriter);
                    }
                }
            } while (currentBatch != null);
        } finally {
            close(this.dataReader);
        }
    }

    protected int forEachTableInBatch(DataContext context, boolean processBatch, Batch batch) {
        int dataRow = 0;
        do {
            currentTable = dataReader.nextTable();
            context.setTable(currentTable);
            if (currentTable != null) {
                boolean processTable = false;
                try {
                    try {
                        if (processBatch) {
                            processTable = context.getWriter().start(currentTable);
                        }
                        dataRow += forEachDataInTable(context, processTable, batch);
                    } catch (IgnoreBatchException ex) {
                        processBatch = false;
                    }
                } finally {
                    if (processTable) {
                        context.getWriter().end(currentTable);
                    }
                }
            }
        } while (currentTable != null);
        return dataRow;
    }

    protected int forEachDataInTable(DataContext context, boolean processTable, Batch batch) {
        int dataRow = 0;
        IgnoreBatchException ignore = null;
        do {
            batch.startTimer(STAT_READ_DATA);
            currentData = dataReader.nextData();
            context.setData(currentData);
            batch.incrementDataReadMillis(batch.endTimer(STAT_READ_DATA));
            if (currentData != null) {
                dataRow++;
                if (processTable || !currentData.requiresTable()) {
                    try {
                        batch.startTimer(STAT_WRITE_DATA);
                        batch.incrementLineCount();
                        context.getWriter().write(currentData);
                        batch.incrementDataWriteMillis(batch.endTimer(STAT_WRITE_DATA));
                    } catch (IgnoreBatchException ex) {
                        ignore = ex;
                        processTable = false;
                    }
                }
            }
        } while (currentData != null);

        if (ignore != null) {
            throw ignore;
        }
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
            if (dataResource != null) {
                dataResource.close();
            }
        } catch (Exception ex) {
            log.error(ex.getMessage(), ex);
        }
    }

    public void setListener(IDataProcessorListener listener) {
        this.listener = listener;
    }

    public void setDataReader(IDataReader dataReader) {
        this.dataReader = dataReader;
    }

    public void setDefaultDataWriter(IDataWriter dataWriter) {
        this.defaultDataWriter = dataWriter;
    }

}
