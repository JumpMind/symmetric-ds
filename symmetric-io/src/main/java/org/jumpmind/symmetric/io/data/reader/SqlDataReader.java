package org.jumpmind.symmetric.io.data.reader;

import java.io.File;
import java.io.InputStream;
import java.io.Reader;

import org.apache.commons.io.IOUtils;
import org.jumpmind.db.sql.SqlScriptReader;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataEventType;

/**
 * Reads a SQL script and passes each SQL statement through the reader as a
 * {@link CsvData} event.
 */
public class SqlDataReader extends AbstractTableDataReader {

    protected SqlScriptReader sqlScriptReader;

    public SqlDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            File file) {
        super(batch, catalogName, schemaName, tableName, file);
    }

    public SqlDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            InputStream is) {
        super(batch, catalogName, schemaName, tableName, is);
    }

    public SqlDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            Reader reader) {
        super(batch, catalogName, schemaName, tableName, reader);
    }

    public SqlDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            String input) {
        super(batch, catalogName, schemaName, tableName, input);
    }

    public SqlDataReader(Batch batch, String catalogName, String schemaName, String tableName,
            StringBuilder input) {
        super(batch, catalogName, schemaName, tableName, input);
    }

    public SqlDataReader(BinaryEncoding binaryEncoding, String catalogName, String schemaName,
            String tableName, InputStream is) {
        super(binaryEncoding, catalogName, schemaName, tableName, is);
    }

    public SqlDataReader(BinaryEncoding binaryEncoding, String catalogName, String schemaName,
            String tableName, Reader reader) {
        super(binaryEncoding, catalogName, schemaName, tableName, reader);
    }

    @Override
    protected void init() {
        /*
         * Tables are really relevant as we aren't going to parse each SQL
         * statement.
         */
        this.readDataBeforeTable = true;
        this.sqlScriptReader = new SqlScriptReader(reader);
    }

    @Override
    protected CsvData readNext() {
        String sql = sqlScriptReader.readSqlStatement();
        if (sql != null) {
            return new CsvData(DataEventType.SQL, new String[] { sql });
        } else {
            return null;
        }
    }

    @Override
    protected void finish() {
        IOUtils.closeQuietly(this.sqlScriptReader);
    }

}
