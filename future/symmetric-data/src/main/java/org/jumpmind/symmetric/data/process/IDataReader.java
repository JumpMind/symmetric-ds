package org.jumpmind.symmetric.data.process;

import org.jumpmind.symmetric.data.model.Batch;
import org.jumpmind.symmetric.data.model.Data;
import org.jumpmind.symmetric.data.model.Table;

public interface IDataReader<T extends DataContext> {

    public void open(T context);

    public Batch nextBatch(T context);

    public Table nextTable(T context);

    public Data nextData(T context);

    public void close(T context);

    public T createDataContext();
}
