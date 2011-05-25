package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataReader {

    public void open(DataContext context);

    public Batch nextBatch();

    public Table nextTable();

    public Data nextData();

    public void close();
    
}
