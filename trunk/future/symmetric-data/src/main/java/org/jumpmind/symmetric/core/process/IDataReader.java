package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataReader<T extends DataContext> {

    public void open(T context);

    public Batch nextBatch(T context);

    public Table nextTable(T context);

    public Data nextData(T context);

    public void close(T context);

    public T createDataContext();
}
