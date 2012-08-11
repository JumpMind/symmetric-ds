package org.jumpmind.symmetric.io.data.reader;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.util.Statistics;

/**
 * Read CSV formatted data for a single table. Requires that the column names be
 * the header of the CSV.
 */
public class TableCsvDataReader extends AbstractCsvDataReader implements IDataReader {

    public static final String CTX_LINE_NUMBER = TableCsvDataReader.class.getSimpleName()
            + ".lineNumber";

    protected Reader reader;
    protected Statistics statistics = new Statistics();
    protected CsvReader csvReader;
    protected DataContext context;
    protected Batch batch;
    protected Table table;
    protected int lineNumber = 0;

    protected boolean readingBatch = false;
    protected boolean readingTable = false;

    public TableCsvDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            StringBuilder input) {
        this(batch, catalogName, schemaName, tableName, new BufferedReader(new StringReader(
                input.toString())));
    }

    public TableCsvDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            InputStream is) {
        this(batch, catalogName, schemaName, tableName, toReader(is));
    }

    public TableCsvDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            String input) {
        this(batch, catalogName, schemaName, tableName, new BufferedReader(new StringReader(input)));
    }

    public TableCsvDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            File file) {
        this(batch, catalogName, schemaName, toTableName(tableName, file), toReader(file));
    }

    public TableCsvDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            Reader reader) {
        this.reader = reader;
        this.batch = batch;
        this.table = new Table(catalogName, schemaName, tableName);
    }

    public TableCsvDataReader(BinaryEncoding binaryEncoding, String catalogName, String schemaName,
            String tableName, Reader reader) {
        this(toBatch(binaryEncoding), catalogName, schemaName, tableName, reader);
    }

    public TableCsvDataReader(BinaryEncoding binaryEncoding, String catalogName, String schemaName,
            String tableName, InputStream is) {
        this(toBatch(binaryEncoding), catalogName, schemaName, tableName, is);
    }

    protected static String toTableName(String tableName, File file) {
        if (StringUtils.isBlank(tableName)) {
            tableName = file.getName();
            if (tableName.lastIndexOf(".") > 0) {
                tableName = tableName.substring(0, tableName.lastIndexOf("."));
            }
        }
        return tableName;
    }

    protected static Batch toBatch(BinaryEncoding binaryEncoding) {
        return new Batch(BatchType.LOAD, Batch.UNKNOWN_BATCH_ID, "default", binaryEncoding, null,
                null, true);
    }

    protected static Reader toReader(File file) {
        try {
            FileInputStream fis = new FileInputStream(file);
            InputStreamReader in = new InputStreamReader(fis, "UTF-8");
            return new BufferedReader(in);
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    protected static Reader toReader(InputStream is) {
        try {
            return new BufferedReader(new InputStreamReader(is, "UTF-8"));
        } catch (IOException ex) {
            throw new IoException(ex);
        }
    }

    public void open(DataContext context) {
        try {
            this.lineNumber = 0;
            this.context = context;
            this.csvReader = CsvUtils.getCsvReader(reader);
            this.csvReader.setUseComments(true);
            this.csvReader.readHeaders();
            String[] columnNames = this.csvReader.getHeaders();
            for (String columnName : columnNames) {
                table.addColumn(new Column(columnName));
            }
        } catch (IOException e) {
            throw new IoException(e);
        }
    }

    public CsvData nextData() {
        if (readingTable) {
            try {
                if (csvReader.readRecord()) {
                    lineNumber++;
                    context.put(CTX_LINE_NUMBER, lineNumber);
                    String[] tokens = csvReader.getValues();
                    statistics.increment(DataReaderStatistics.READ_BYTE_COUNT,
                            logDebugAndCountBytes(tokens));
                    return new CsvData(DataEventType.INSERT, tokens);
                }
            } catch (IOException ex) {
                throw new IoException(ex);
            }
            batch.setComplete(true);
        }
        return null;

    }

    public Batch nextBatch() {
        if (!readingBatch) {
            readingBatch = true;
            return batch;
        } else {
            return null;
        }
    }

    public Table nextTable() {
        if (!readingTable) {
            readingTable = true;
            return table;
        } else {            
            return null;
        }
    }

    public void close() {
        if (csvReader != null) {
            csvReader.close();
        }
    }

    public Map<Batch, Statistics> getStatistics() {
        Map<Batch, Statistics> map = new HashMap<Batch, Statistics>(1);
        map.put(batch, statistics);
        return map;
    }
}
