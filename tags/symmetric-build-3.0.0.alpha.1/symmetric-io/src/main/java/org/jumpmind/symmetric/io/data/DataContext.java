package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;
import org.jumpmind.util.Context;

public class DataContext extends Context {

    protected IDataWriter writer;

    protected IDataReader reader;

    protected Batch batch;
    
    protected Table table;
    
    protected CsvData data;

    public DataContext(Batch batch) {
        this.batch = batch;
    }

    public DataContext() {
    }

    public DataContext(IDataReader reader) {
        this.reader = reader;
    }

    public IDataReader getReader() {
        return reader;
    }

    public IDataWriter getWriter() {
        return writer;
    }
    
    protected void setWriter(IDataWriter writer) {
        this.writer = writer;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Batch getBatch() {
        return batch;
    }
    
    public void setData(CsvData data) {
        this.data = data;
    }
    
    public CsvData getData() {
        return data;
    }
    
    public void setTable(Table table) {
        this.table = table;
    }
    
    public Table getTable() {
        return table;
    }

}
