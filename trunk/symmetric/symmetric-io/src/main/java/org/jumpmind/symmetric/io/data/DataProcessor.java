package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DataProcessor {

    private static final String STAT_WRITE_DATA = "statWriteData";
    private static final String STAT_READ_DATA = "statReadData";

    static final Logger log = LoggerFactory.getLogger(DataProcessor.class);

    protected IDataReader dataReader;

    protected IDataWriter defaultDataWriter;

    protected IDataProcessorListener listener;

    public DataProcessor() {
    }

    public DataProcessor(IDataReader dataReader, IDataWriter dataWriter) {
        this(dataReader, dataWriter, null);
    }

    public DataProcessor(IDataReader dataReader, IDataWriter defaultDataWriter,
            IDataProcessorListener listener) {
        this.dataReader = dataReader;
        this.defaultDataWriter = defaultDataWriter;
        this.listener = listener;
    }

    protected IDataWriter chooseDataWriter(Batch batch) {
        return this.defaultDataWriter;
    }

    public void process() {
        try {
            DataContext context = new DataContext(dataReader);
            dataReader.open(context);
            Batch batch = null;
            do {
                batch = dataReader.nextBatch();
                if (batch != null) {
                    context.setBatch(batch);
                    boolean endBatchCalled = false;
                    IDataWriter dataWriter = null;
                    try {
                        int dataRow = 0;
                        boolean processBatch = listener == null ? true : listener
                                .beforeBatchStarted(context);

                        if (processBatch) {
                            dataWriter = chooseDataWriter(batch);
                            processBatch &= dataWriter != null;
                        }

                        if (processBatch) {
                            context.setWriter(dataWriter);
                            dataWriter.open(context);
                            dataWriter.start(batch);
                            if (listener != null) {
                                listener.afterBatchStarted(context);
                            }
                        }

                        // pull and process any data events that are not wrapped
                        // in a table
                        dataRow += forEachDataInTable(context, processBatch, batch);

                        // pull and process all data events wrapped in tables
                        dataRow += forEachTableInBatch(context, processBatch, batch);

                        if (processBatch) {
                            if (listener != null) {
                                listener.beforeBatchEnd(context);
                            }
                            dataWriter.end(batch, false);
                            endBatchCalled = true;
                            if (listener != null) {
                                listener.batchSuccessful(context);
                            }
                        }
                    } catch (Exception ex) {
                        try {
                            if (listener != null) {
                                listener.batchInError(context, ex);
                            }
                        } finally {
                            if (dataWriter != null && !endBatchCalled) {
                                dataWriter.end(batch, true);
                            }
                        }
                        rethrow(ex);
                    } finally {
                        close(dataWriter);
                    }
                }
            } while (batch != null);
        } finally {
            close(this.dataReader);
        }
    }

    protected int forEachTableInBatch(DataContext context, boolean processBatch, Batch batch) {
        int dataRow = 0;
        Table table = null;
        do {
            table = dataReader.nextTable();
            if (table != null) {
                boolean processTable = false;
                try {
                    if (processBatch) {
                        processTable = context.getWriter().start(table);
                    }
                    dataRow += forEachDataInTable(context, processTable, batch);
                } finally {
                    if (processTable) {
                        context.getWriter().end(table);
                    }
                }
            }
        } while (table != null);
        return dataRow;
    }

    protected int forEachDataInTable(DataContext context, boolean processTable, Batch batch) {
        int dataRow = 0;
        CsvData data = null;
        do {
            batch.startTimer(STAT_READ_DATA);
            data = dataReader.nextData();
            batch.incrementDataReadMillis(batch.endTimer(STAT_READ_DATA));
            if (data != null) {
                dataRow++;
                if (processTable) {
                    batch.startTimer(STAT_WRITE_DATA);
                    batch.incrementLineCount();
                    context.getWriter().write(data);
                    batch.incrementDataWriteMillis(batch.endTimer(STAT_WRITE_DATA));
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
