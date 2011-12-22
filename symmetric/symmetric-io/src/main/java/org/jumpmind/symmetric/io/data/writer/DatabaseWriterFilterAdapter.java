package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class DatabaseWriterFilterAdapter implements IDatabaseWriterFilter {
    
    protected boolean autoRegister = true;

    public boolean isAutoRegister() {
        return autoRegister;
    }
    
    public void setAutoRegister(boolean autoRegister) {
        this.autoRegister = autoRegister;
    }

    public <R extends IDataReader, W extends IDataWriter> boolean beforeWrite(
            DataContext<R, W> context, Table table, CsvData data) {
        return true;
    }

    public <R extends IDataReader, W extends IDataWriter> void afterWrite(
            DataContext<R, W> context, Table table, CsvData data) {
    }

    public <R extends IDataReader, W extends IDataWriter> boolean handlesMissingTable(
            DataContext<R, W> context, Table table) {
        return false;
    }

    public <R extends IDataReader, W extends IDataWriter> void earlyCommit(
            DataContext<R, W> context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchComplete(
            DataContext<R, W> context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchCommitted(
            DataContext<R, W> context) {
    }

    public <R extends IDataReader, W extends IDataWriter> void batchRolledback(
            DataContext<R, W> context) {
    }

}
