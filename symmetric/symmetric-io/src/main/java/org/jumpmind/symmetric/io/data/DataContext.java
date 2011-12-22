package org.jumpmind.symmetric.io.data;

import org.jumpmind.util.Context;

public class DataContext<R extends IDataReader, W extends IDataWriter> extends Context {

    protected W writer;

    protected R reader;

    protected Batch batch;

    public DataContext(Batch batch) {
        this.batch = batch;
    }

    public DataContext() {
    }

    public DataContext(R reader, W writer) {
        this.writer = writer;
        this.reader = reader;
    }

    public R getReader() {
        return reader;
    }

    public W getWriter() {
        return writer;
    }

    public void setBatch(Batch batch) {
        this.batch = batch;
    }

    public Batch getBatch() {
        return batch;
    }

}
