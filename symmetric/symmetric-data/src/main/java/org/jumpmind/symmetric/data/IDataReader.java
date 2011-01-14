package org.jumpmind.symmetric.data;

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.model.Batch;
import org.jumpmind.symmetric.model.Data;

public interface IDataReader<T extends DataContext> {

    public void open(T context);

    public Batch nextBatch(T context);

    public Table nextTable(T context);

    public Data nextData(T context);

    public void close(T context);
    
    public T createDataContext();
}
