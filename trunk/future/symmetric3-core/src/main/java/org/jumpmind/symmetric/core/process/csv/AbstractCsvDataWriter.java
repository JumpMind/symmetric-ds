package org.jumpmind.symmetric.core.process.csv;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.AbstractDataWriter;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.IDataWriter;
import org.jumpmind.symmetric.csv.CsvConstants;

abstract public class AbstractCsvDataWriter extends AbstractDataWriter implements IDataWriter {

    protected DataContext context;

    protected Batch batch;

    protected Table table;

    protected Set<Table> processedTables = new HashSet<Table>();

    protected String delimiter = ",";

    protected boolean flushNodeId = true;

    public AbstractCsvDataWriter() {
    }

    public AbstractCsvDataWriter(IDataFilter... filters) {
        this.dataFilters = toList(filters);
    }

    public void open(DataContext context) {
        this.context = context;
    }

    public void startBatch(Batch batch) {
        this.batch = batch;
        if (flushNodeId) {
            String sourceNodeId = context.getSourceNodeId();
            if (StringUtils.isNotBlank(sourceNodeId)) {
                println(CsvConstants.NODEID, sourceNodeId);
            }
            BinaryEncoding binaryEncoding = context.getBinaryEncoding();
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

    public boolean writeTable(Table table) {
        this.table = table;
        println(CsvConstants.TABLE, table.getTableName());
        String schemaName = table.getSchemaName();
        println(CsvConstants.SCHEMA, StringUtils.isNotBlank(schemaName) ? schemaName : "");
        String catalogName = table.getCatalogName();
        println(CsvConstants.CATALOG, StringUtils.isNotBlank(catalogName) ? catalogName : "");
        if (!processedTables.contains(table)) {
            println(CsvConstants.KEYS, table.getPrimaryKeyColumns());
            println(CsvConstants.COLUMNS, table.getColumns());
        }
        this.processedTables.add(table);
        return true;
    }

    public boolean writeData(Data data) {
        if (filterData(data, batch, table, context)) {
            switch (data.getEventType()) {
            case INSERT:
                println(CsvConstants.INSERT, data.getRowData());
                break;

            case UPDATE:
                println(CsvConstants.UPDATE, data.getRowData(), data.getPkData());
                if (StringUtils.isNotBlank(data.getOldData())) {
                    println(CsvConstants.OLD, data.getOldData());
                }
                break;

            case DELETE:
                println(CsvConstants.DELETE, data.getPkData());
                break;

            case SQL:
                println(CsvConstants.SQL, data.getRowData());
                break;
            }
        }

        return false;
    }

    public void finishBatch(Batch batch) {
        println(CsvConstants.COMMIT);
    }

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

    abstract protected void print(String data);

    protected int println(String... data) {
        StringBuilder buffer = new StringBuilder();
        for (int i = 0; i < data.length; i++) {
            if (i != 0) {
                buffer.append(delimiter);
            }
            buffer.append(data[i]);
        }
        buffer.append("\n");
        print(buffer.toString());
        return buffer.length();
    }

    public void setDelimiter(String delimiter) {
        this.delimiter = delimiter;
    }

    public String getDelimiter() {
        return delimiter;
    }

}
