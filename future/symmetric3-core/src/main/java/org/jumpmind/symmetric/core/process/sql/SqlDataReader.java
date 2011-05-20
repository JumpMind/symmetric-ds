package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataReader;

public class SqlDataReader implements IDataReader<DataContext> {

    public void close(DataContext context) {
    }

    public DataContext createDataContext() {

        return null;
    }

    public Batch nextBatch(DataContext context) {

        return null;
    }

    public Data nextData(DataContext context) {

        return null;
    }

    public Table nextTable(DataContext context) {

        return null;
    }

    public void open(DataContext context) {

    }
}
