package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;

public class DatabaseWriterFilterAdapter implements IDatabaseWriterFilter {

    public boolean beforeWrite(DataContext context, Table table, CsvData data) {
        return true;
    }

    public void afterWrite(DataContext context, Table table, CsvData data) {
    }

    public boolean handlesMissingTable(DataContext context, Table table) {
        return false;
    }

    public void earlyCommit(DataContext context) {
    }

    public void batchComplete(DataContext context) {
    }

    public void batchCommitted(DataContext context) {
    }

    public void batchRolledback(DataContext context) {
    }

}
