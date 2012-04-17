package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class DatabaseWriterFilterAdapter implements IDatabaseWriterFilter {
    
    public <R extends IDataReader, W extends IDataWriter> boolean beforeWrite(
            DataContext context, Table table, CsvData data) {
        return true;
    }

    public <R extends IDataReader, W extends IDataWriter> void afterWrite(
            DataContext context, Table table, CsvData data) {
    }

    public <R extends IDataReader, W extends IDataWriter> boolean handlesMissingTable(
            DataContext context, Table table) {
        return false;
    }

    public <R extends IDataReader, W extends IDataWriter> void earlyCommit(
            DataContext context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchComplete(
            DataContext context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchCommitted(
            DataContext context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchRolledback(
            DataContext context) {
    }

}
