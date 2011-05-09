package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.IDataReader;
import org.jumpmind.symmetric.core.sql.ISqlReadCursor;
import org.jumpmind.symmetric.core.sql.mapper.DataMapper;

public class SqlTableDataReader implements IDataReader<SqlDataContext> {

    enum Status {
        New, BatchStarted, TableStarted, DataStarted, Finished
    };

    protected TableToExtract tableToRead;
    protected IDbPlatform platform;
    protected Batch batch;
    protected Status status = Status.New;
    protected ISqlReadCursor<Data> readCursor;

    public SqlTableDataReader(IDbPlatform platform, Batch batch, TableToExtract tableToRead) {
        validateArgs(platform, batch, tableToRead);
        this.tableToRead = tableToRead;
        this.platform = platform;
        this.batch = batch;
    }

    protected void validateArgs(IDbPlatform platform, Batch batch, TableToExtract tableToRead) {
        String nullMsg = "";
        if (platform == null) {
            nullMsg += IDbPlatform.class.getName() + " must not be null.  ";
        }
        if (batch == null) {
            nullMsg += Batch.class.getName() + " must not be null.  ";
        }
        if (tableToRead == null) {
            nullMsg += TableToExtract.class.getName() + " must not be null.  ";
        }
        if (nullMsg != null) {
            throw new NullPointerException(nullMsg);
        }
    }

    public void close(SqlDataContext context) {
        if (readCursor != null) {
            readCursor.close();
        }
    }

    public SqlDataContext createDataContext() {
        return new SqlDataContext();
    }

    public Batch nextBatch(SqlDataContext context) {
        if (status == Status.New) {
            status = Status.BatchStarted;
            return batch;
        } else {
            return null;
        }
    }

    public Data nextData(SqlDataContext context) {
        Data data = null;
        if (status == Status.TableStarted) {
            status = Status.DataStarted;
            // TODO sql template?
            this.readCursor = this.platform.getSqlConnection().queryForObject(null,
                    new DataMapper());
        }
        if (readCursor != null) {
            data = readCursor.next();
        }
        return data;
    }

    public Table nextTable(SqlDataContext context) {
        if (status == Status.BatchStarted) {
            status = Status.TableStarted;
            return tableToRead.getTable();
        } else {
            return null;
        }
    }

    public void open(SqlDataContext context) {
    }
}
