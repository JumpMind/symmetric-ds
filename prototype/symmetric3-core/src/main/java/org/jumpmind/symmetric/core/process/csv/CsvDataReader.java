package org.jumpmind.symmetric.core.process.csv;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.jumpmind.symmetric.core.common.ArrayUtils;
import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.common.IoException;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataReader;
import org.jumpmind.symmetric.csv.CsvConstants;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.csv.CsvUtils;

public class CsvDataReader implements IDataReader {

    protected Reader reader;
    protected CsvReader csvReader;
    protected DataContext context;
    protected Map<String, Table> tables = new HashMap<String, Table>();
    protected Object next;
    protected Batch batch;
    protected Table table;

    public CsvDataReader(StringBuilder input) {
        this(new BufferedReader(new StringReader(input.toString())));
    }

    public CsvDataReader(String input) {
        this(new BufferedReader(new StringReader(input)));
    }

    public CsvDataReader(Reader reader) {
        this.reader = reader;
    }

    public CsvDataReader(File file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader in = new InputStreamReader(fis, "UTF-8");
            this.reader = new BufferedReader(in);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public void open(DataContext context) {
        this.context = context;
        this.csvReader = CsvUtils.getCsvReader(reader);
        this.next = readNext();
    }

    protected Object readNext() {
        try {
            String channelId = null;
            Set<String> keys = null;
            String schemaName = null;
            String catalogName = null;
            String[] parsedOldData = null;
            long bytesRead = 0;
            while (csvReader.readRecord()) {
                String[] tokens = csvReader.getValues();
                if (batch == null) {
                    bytesRead += csvReader.getRawRecord().length();
                } else {
                    batch.incrementReadByteCount(csvReader.getRawRecord().length() + bytesRead);
                    bytesRead = 0;
                }
                if (tokens[0].equals(CsvConstants.BATCH)) {
                    return new Batch(Long.parseLong(tokens[1]), channelId);
                } else if (tokens[0].equals(CsvConstants.NODEID)) {
                    this.context.setSourceNodeId(tokens[1]);
                } else if (tokens[0].equals(CsvConstants.BINARY)) {
                    context.setBinaryEncoding(BinaryEncoding.valueOf(tokens[1]));
                } else if (tokens[0].equals(CsvConstants.CHANNEL)) {
                    channelId = tokens[1];
                } else if (tokens[0].equals(CsvConstants.SCHEMA)) {
                    schemaName = StringUtils.isBlank(tokens[1]) ? null : tokens[1];
                } else if (tokens[0].equals(CsvConstants.CATALOG)) {
                    catalogName = StringUtils.isBlank(tokens[1]) ? null : tokens[1];
                } else if (tokens[0].equals(CsvConstants.TABLE)) {
                    String tableName = tokens[1];
                    table = tables.get(Table.getFullyQualifiedTableName(tableName, schemaName,
                            catalogName, ""));
                    if (table != null) {
                        return table;
                    } else {
                        table = new Table(catalogName, schemaName, tableName);
                    }
                } else if (tokens[0].equals(CsvConstants.KEYS)) {
                    if (keys == null) {
                        keys = new HashSet<String>(tokens.length);
                    }
                    for (int i = 1; i < tokens.length; i++) {
                        keys.add(tokens[i]);
                    }
                } else if (tokens[0].equals(CsvConstants.COLUMNS)) {
                    for (int i = 1; i < tokens.length; i++) {
                        Column column = new Column(tokens[i], keys != null
                                && keys.contains(tokens[i]));
                        table.addColumn(column);
                    }
                    tables.put(table.getFullyQualifiedTableName(""), table);
                    return table;
                } else if (tokens[0].equals(CsvConstants.COMMIT)) {
                    return null;
                } else if (tokens[0].equals(CsvConstants.INSERT)) {
                    Data data = new Data();
                    data.setEventType(DataEventType.INSERT);
                    data.setChannelId(batch.getChannelId());
                    data.putParsedRowData(ArrayUtils.subarray(tokens, 1, tokens.length));
                    return data;
                } else if (tokens[0].equals(CsvConstants.OLD)) {
                    parsedOldData = ArrayUtils.subarray(tokens, 1, tokens.length);
                } else if (tokens[0].equals(CsvConstants.UPDATE)) {
                    Data data = new Data();
                    data.setEventType(DataEventType.UPDATE);
                    data.setChannelId(batch.getChannelId());
                    data.putParsedRowData(ArrayUtils.subarray(tokens, 1, table.getColumnCount() + 1));
                    data.putParsedPkData(ArrayUtils.subarray(tokens, table.getColumnCount() + 1,
                            tokens.length));
                    data.putParsedOldData(parsedOldData);
                    return data;
                } else if (tokens[0].equals(CsvConstants.DELETE)) {
                    Data data = new Data();
                    data.setEventType(DataEventType.DELETE);
                    data.setChannelId(batch.getChannelId());
                    data.putParsedPkData(ArrayUtils.subarray(tokens, 1, tokens.length));
                    data.putParsedOldData(parsedOldData);
                    return data;
                } else if (tokens[0].equals(CsvConstants.SQL)) {
                    Data data = new Data();
                    data.setEventType(DataEventType.SQL);
                    data.setChannelId(batch.getChannelId());
                    data.setRowData(tokens[1]);
                    return data;
                } else {
                    // TODO log that we received a unknown set of tokens
                }
            }
        } catch (IOException ex) {
            throw new IoException(ex);
        }

        return null;

    }

    public Batch nextBatch() {
        if (next instanceof Batch) {
            this.batch = (Batch) next;
            next = null;
            return batch;
        } else {
            do {
                next = readNext();
                if (next instanceof Batch) {
                    this.batch = (Batch) next;
                    next = null;
                    return batch;
                }
            } while (next != null);
        }
        return null;
    }

    public Table nextTable() {
        if (next instanceof Table) {
            this.table = (Table) next;
            next = null;
            return table;
        } else {
            do {
                next = readNext();
                if (next instanceof Table) {
                    this.table = (Table) next;
                    next = null;
                    return table;
                }
            } while (next != null && !(next instanceof Batch));
        }
        return null;
    }

    public Data nextData() {
        if (next instanceof Data) {
            Data data = (Data) next;
            next = null;
            return data;
        } else {
            do {
                next = readNext();
                if (next instanceof Data) {
                    Data data = (Data) next;
                    next = null;
                    return data;
                }
            } while (next != null && !(next instanceof Batch) && !(next instanceof Table));
        }
        return null;
    }

    public void close() {
        if (csvReader != null) {
            csvReader.close();
        }
    }

}
