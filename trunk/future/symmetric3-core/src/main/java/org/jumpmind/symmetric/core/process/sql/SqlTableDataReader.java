package org.jumpmind.symmetric.core.process.sql;

import java.util.List;

import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.IDataReader;

public class SqlTableDataReader implements IDataReader<SqlDataContext> {

    protected List<TableToRead> tablesToRead;
    protected IPlatform platform;

    public SqlTableDataReader(IPlatform platform, List<TableToRead> tablesToRead) {
        this.tablesToRead = tablesToRead;
        this.platform = platform;
    }

    public void close(SqlDataContext context) {

    }

    public SqlDataContext createDataContext() {
        return null;
    }

    public Batch nextBatch(SqlDataContext context) {
        return null;
    }

    public Data nextData(SqlDataContext context) {
        return null;
    }

    public Table nextTable(SqlDataContext context) {
        return null;
    }

    public void open(SqlDataContext context) {

    }
}
