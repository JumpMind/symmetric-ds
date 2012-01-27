package org.jumpmind.symmetric.io.data.writer;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvConstants;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

abstract public class AbstractProtocolDataWriter implements IDataWriter {

    protected DataContext context;

    protected Batch batch;

    protected Table table;

    protected Set<Table> processedTables = new HashSet<Table>();

    protected String delimiter = ",";

    protected boolean flushNodeId = true;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected List<IProtocolDataWriterListener> listeners;

    public AbstractProtocolDataWriter(List<IProtocolDataWriterListener> listeners) {
        this.listeners = listeners;
    }

    public void open(DataContext context) {
        this.context = context;
    }

    public void close() {
    }

    public void start(Batch batch) {
        this.statistics.put(batch, new Statistics());
        this.batch = batch;

        if (listeners != null) {
            for (IProtocolDataWriterListener listener : listeners) {
                listener.start(batch);
            }
        }

        if (flushNodeId) {
            String sourceNodeId = batch.getNodeId();
            if (StringUtils.isNotBlank(sourceNodeId)) {
                println(CsvConstants.NODEID, sourceNodeId);
            }
            BinaryEncoding binaryEncoding = batch.getBinaryEncoding();
            if (binaryEncoding != null) {
                println(CsvConstants.BINARY, binaryEncoding.name());
            }
            flushNodeId = false;
        }
        if (StringUtils.isNotBlank(batch.getChannelId())) {
            println(CsvConstants.CHANNEL, batch.getChannelId());
        }
        println(CsvConstants.BATCH, Long.toString(batch.getBatchId()));
    }

    public boolean start(Table table) {
        this.table = table;
        String catalogName = table.getCatalog();
        println(CsvConstants.CATALOG, StringUtils.isNotBlank(catalogName) ? catalogName : "");
        String schemaName = table.getSchema();
        println(CsvConstants.SCHEMA, StringUtils.isNotBlank(schemaName) ? schemaName : "");
        println(CsvConstants.TABLE, table.getName());
        if (!processedTables.contains(table)) {
            println(CsvConstants.KEYS, table.getPrimaryKeyColumns());
            println(CsvConstants.COLUMNS, table.getColumns());
        }
        this.processedTables.add(table);
        return true;
    }

    public void write(CsvData data) {
        switch (data.getDataEventType()) {
        case INSERT:
            println(CsvConstants.INSERT, data.getCsvData(CsvData.ROW_DATA));
            break;

        case UPDATE:
            String oldData = data.getCsvData(CsvData.OLD_DATA);
            if (StringUtils.isNotBlank(oldData)) {
                println(CsvConstants.OLD, oldData);
            }
            println(CsvConstants.UPDATE, data.getCsvData(CsvData.ROW_DATA),
                    data.getCsvData(CsvData.PK_DATA));
            break;

        case DELETE:
            oldData = data.getCsvData(CsvData.OLD_DATA);
            if (StringUtils.isNotBlank(oldData)) {
                println(CsvConstants.OLD, oldData);
            }
            println(CsvConstants.DELETE, data.getCsvData(CsvData.PK_DATA));
            break;

        case SQL:
            println(CsvConstants.SQL, data.getCsvData(CsvData.ROW_DATA));
            break;
        }
    }

    public void end(Table table) {
    }

    final public void end(Batch batch, boolean inError) {
        if (!inError) {
            println(CsvConstants.COMMIT, Long.toString(batch.getBatchId()));

            endBatch(batch);
        }

        if (listeners != null) {
            for (IProtocolDataWriterListener listener : listeners) {
                notifyEndBatch(batch, listener);
            }
        }
    }

    abstract protected void endBatch(Batch batch);

    abstract protected void notifyEndBatch(Batch batch, IProtocolDataWriterListener listener);

    protected int println(String key, List<Column> columns) {
        return println(key, columns.toArray(new Column[columns.size()]));
    }

    protected int println(String key, Column[] columns) {
        StringBuilder buffer = new StringBuilder(key);
        for (int i = 0; i < columns.length; i++) {
            buffer.append(delimiter);
            buffer.append(columns[i].getName());
        }
        println(buffer.toString());
        return buffer.length();
    }

    abstract protected void print(Batch batch, String data);

    protected int println(String... data) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            if (i != 0) {
                buffer.append(delimiter);
            }
            buffer.append(data[i]);
        }
        buffer.append("\n");
        print(batch, buffer.toString());
        return buffer.length();
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

}
