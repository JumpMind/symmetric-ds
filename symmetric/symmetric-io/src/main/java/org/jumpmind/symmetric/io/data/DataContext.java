package org.jumpmind.symmetric.io.data;

import org.jumpmind.util.Context;

public class DataContext extends Context {

    protected IDataWriter writer;

    protected IDataReader reader;

    protected Batch batch;

    public DataContext(Batch batch) {
        this.batch = batch;
    }

    public DataContext() {
    }

    public DataContext(IDataReader reader, IDataWriter writer) {
        this.writer = writer;
        this.reader = reader;
    }

    public IDataReader getReader() {
        return reader;
    }

    public IDataWriter getWriter() {
        return writer;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Batch getBatch() {
        return batch;
    }

}
