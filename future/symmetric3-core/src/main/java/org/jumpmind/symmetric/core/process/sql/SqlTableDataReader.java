package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.db.TriggerBuilder;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataReader;
import org.jumpmind.symmetric.core.sql.ISqlConnection;
import org.jumpmind.symmetric.core.sql.ISqlReadCursor;
import org.jumpmind.symmetric.core.sql.mapper.DataMapper;

public class SqlTableDataReader implements IDataReader<DataContext> {

    enum Status {
        New, BatchStarted, TableStarted, DataStarted, Finished
    };

    protected TableToExtract tableToRead;
    protected IDbPlatform dbPlatform;
    protected Batch batch;
    protected Status status = Status.New;
    protected ISqlReadCursor<Data> readCursor;

    public SqlTableDataReader(IDbPlatform dbPlatform, Batch batch, TableToExtract tableToRead) {
        validateArgs(dbPlatform, batch, tableToRead);
        this.tableToRead = tableToRead;
        this.dbPlatform = dbPlatform;
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
        if (StringUtils.isNotBlank(nullMsg)) {
            throw new NullPointerException(nullMsg);
        }
    }

    public void close(DataContext context) {
        if (readCursor != null) {
            readCursor.close();
        }
    }

    public DataContext createDataContext() {
        return new DataContext();
    }

    public Batch nextBatch(DataContext context) {
        if (status == Status.New) {
            status = Status.BatchStarted;
            return batch;
        } else {
            return null;
        }
    }

    public Data nextData(DataContext context) {
        Data data = null;
        if (status == Status.TableStarted) {
            status = Status.DataStarted;
            ISqlConnection connection = this.dbPlatform.getSqlConnection();
            TriggerBuilder triggerBuilder = this.dbPlatform.getTriggerBuilder();
            String sql = triggerBuilder.createTableExtractSql(tableToRead, this.dbPlatform
                    .getParameters().is(Parameters.DB_SUPPORT_BIG_LOBS, false));
            this.readCursor = connection.queryForCursor(sql, new DataMapper());
        }
        if (readCursor != null) {
            data = readCursor.next();
        }
        return data;
    }

    public Table nextTable(DataContext context) {
        if (status == Status.BatchStarted) {
            status = Status.TableStarted;
            return tableToRead.getTable();
        } else {
            return null;
        }
    }

    public void open(DataContext context) {
    }
}
