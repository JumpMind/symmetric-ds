package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDataCaptureBuilder;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.ISqlReadCursor;
import org.jumpmind.symmetric.core.db.ISqlRowMapper;
import org.jumpmind.symmetric.core.db.ISqlTemplate;
import org.jumpmind.symmetric.core.db.Row;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataReader;

public class SqlTableDataReader implements IDataReader {

    enum Status {
        New, BatchStarted, TableStarted, DataStarted, Finished
    };

    protected TableToExtract tableToRead;
    protected IDbDialect dbDialect;
    protected Batch batch;
    protected Status status = Status.New;
    protected ISqlReadCursor<Data> readCursor;

    public SqlTableDataReader(IDbDialect dbDialect, Batch batch, TableToExtract tableToRead) {
        validateArgs(dbDialect, batch, tableToRead);
        this.tableToRead = tableToRead;
        this.dbDialect = dbDialect;
        this.batch = batch;
    }

    protected void validateArgs(IDbDialect platform, Batch batch, TableToExtract tableToRead) {
        String nullMsg = "";
        if (platform == null) {
            nullMsg += IDbDialect.class.getName() + " must not be null.  ";
        }
        if (batch == null) {
            nullMsg += Batch.class.getName() + " must not be null.  ";
        }
        if (tableToRead == null) {
            nullMsg += TableToExtract.class.getName() + " must not be null.  ";
        }
        if (StringUtils.isNotBlank(nullMsg)) {
            throw new NullPointerException(nullMsg);
        }
    }

    public void close() {
        if (readCursor != null) {
            readCursor.close();
        }
    }

    public Batch nextBatch() {
        if (status == Status.New) {
            status = Status.BatchStarted;
            return batch;
        } else {
            return null;
        }
    }

    public Data nextData() {
        Data data = null;
        if (status == Status.TableStarted) {
            status = Status.DataStarted;
            ISqlTemplate connection = this.dbDialect.getSqlTemplate();
            IDataCaptureBuilder builder = this.dbDialect.getDataCaptureBuilder();
            Parameters parameters = this.dbDialect.getParameters();
            String sql = 
                builder.createTableExtractSql(tableToRead, parameters,
                    parameters.is(Parameters.DB_SUPPORT_BIG_LOBS, false));
            this.readCursor = connection.queryForCursor(sql, new DataFromSqlTableMapper(tableToRead
                    .getTable().getColumns()));
        }
        if (readCursor != null) {
            data = readCursor.next();
        }
        return data;
    }

    public Table nextTable() {
        if (status == Status.BatchStarted) {
            status = Status.TableStarted;
            return tableToRead.getTable();
        } else {
            return null;
        }
    }

    public void open(DataContext context) {
        context.setBinaryEncoding(dbDialect.getDataCaptureBuilder().getBinaryEncoding());
    }

    class DataFromSqlTableMapper implements ISqlRowMapper<Data> {

        private Column[] columns;

        public DataFromSqlTableMapper(Column[] columns) {
            this.columns = columns;
        }

        public Data mapRow(Row row) {
            Data data = new Data();
            StringBuilder rowData = new StringBuilder();
            if (columns != null && columns.length > 0) {
                for (Column column : columns) {
                    String value = (String)row.get(column.getName());
                    if (value != null) {
                        rowData.append(value);
                    }
                    rowData.append(",");
                }
                data.setRowData(rowData.substring(0, rowData.length() - 1));
            }
            data.setEventType(DataEventType.INSERT);
            return data;
        }

    }
}
