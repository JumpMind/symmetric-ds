package org.jumpmind.symmetric.io.data.reader;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.exception.IoException;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataEventType;

/**
 * Read CSV formatted data for a single table. Requires that the column names be
 * the header of the CSV.
 */
public class CsvTableDataReader extends AbstractTableDataReader {

    protected CsvReader csvReader;

    public CsvTableDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            File file) {
        super(batch, catalogName, schemaName, tableName, file);
    }

    public CsvTableDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            InputStream is) {
        super(batch, catalogName, schemaName, tableName, is);
    }

    public CsvTableDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            Reader reader) {
        super(batch, catalogName, schemaName, tableName, reader);
    }

    public CsvTableDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            String input) {
        super(batch, catalogName, schemaName, tableName, input);
    }

    public CsvTableDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            StringBuilder input) {
        super(batch, catalogName, schemaName, tableName, input);
    }

    public CsvTableDataReader(BinaryEncoding binaryEncoding, String catalogName, String schemaName,
            String tableName, InputStream is) {
        super(binaryEncoding, catalogName, schemaName, tableName, is);
    }

    public CsvTableDataReader(BinaryEncoding binaryEncoding, String catalogName, String schemaName,
            String tableName, Reader reader) {
        super(binaryEncoding, catalogName, schemaName, tableName, reader);
    }

    @Override
    protected void init() {
        try {
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

    @Override
    protected CsvData readNext() {
        try {
            if (csvReader.readRecord()) {
                String[] tokens = csvReader.getValues();
                return buildCsvData(tokens, DataEventType.INSERT);
            } else {
                return null;
            }
        } catch (IOException ex) {
            throw new IoException(ex);
        }

    }

    @Override
    protected void finish() {
        if (csvReader != null) {
            csvReader.close();
        }
    }

}
