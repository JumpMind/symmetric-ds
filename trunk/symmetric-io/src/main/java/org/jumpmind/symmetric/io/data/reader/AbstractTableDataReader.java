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

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.util.Statistics;

/**
 * A data reader that knows it will be reading a single batch and a single
 * table.
 */
abstract public class AbstractTableDataReader extends AbstractDataReader implements IDataReader {

    public static final String CTX_LINE_NUMBER = AbstractTableDataReader.class.getSimpleName()
            + ".lineNumber";

    protected Reader reader;
    protected Statistics statistics = new Statistics();
    protected DataContext context;
    protected Batch batch;
    protected Table table;
    protected int lineNumber = 0;
    protected boolean readDataBeforeTable = false;
    protected boolean readingBatch = false;
    protected boolean readingTable = false;

    public AbstractTableDataReader(Batch batch, String catalogName, String schemaName,
            String tableName, StringBuilder input) {
        this(batch, catalogName, schemaName, tableName, new BufferedReader(new StringReader(
                input.toString())));
    }

    public AbstractTableDataReader(Batch batch, String catalogName, String schemaName,
            String tableName, InputStream is) {
        this(batch, catalogName, schemaName, tableName, toReader(is));
    }

    public AbstractTableDataReader(Batch batch, String catalogName, String schemaName,
            String tableName, String input) {
        this(batch, catalogName, schemaName, tableName, new BufferedReader(new StringReader(input)));
    }

    public AbstractTableDataReader(Batch batch, String catalogName, String schemaName,
            String tableName, File file) {
        this(batch, catalogName, schemaName, toTableName(tableName, file), toReader(file));
    }

    public AbstractTableDataReader(Batch batch, String catalogName, String schemaName,
            String tableName, Reader reader) {
        this.reader = reader;
        this.batch = batch;
        if (StringUtils.isNotBlank(tableName)) {
            this.table = new Table(catalogName, schemaName, tableName);
        }
    }

    public AbstractTableDataReader(BinaryEncoding binaryEncoding, String catalogName,
            String schemaName, String tableName, Reader reader) {
        this(toBatch(binaryEncoding), catalogName, schemaName, tableName, reader);
    }

    public AbstractTableDataReader(BinaryEncoding binaryEncoding, String catalogName,
            String schemaName, String tableName, InputStream is) {
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
        this.lineNumber = 0;
        this.context = context;
        this.init();
    }

    abstract protected void init();

    abstract protected CsvData readNext();

    abstract protected void finish();

    protected CsvData buildCsvData(String[] tokens, DataEventType dml) {
        statistics.increment(DataReaderStatistics.READ_BYTE_COUNT, logDebugAndCountBytes(tokens));
        return new CsvData(dml, tokens);
    }

    public CsvData nextData() {
        if (readDataBeforeTable || readingTable) {
            CsvData data = readNext();
            if (data != null) {
                lineNumber++;
                context.put(CTX_LINE_NUMBER, lineNumber);
                return data;
            } else {
                batch.setComplete(true);
            }
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
        IOUtils.closeQuietly(reader);
        finish();
    }

    public Map<Batch, Statistics> getStatistics() {
        Map<Batch, Statistics> map = new HashMap<Batch, Statistics>(1);
        map.put(batch, statistics);
        return map;
    }
}
