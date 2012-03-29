package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataReader extends IDataResource {

    public Batch nextBatch();

    public Table nextTable();

    public Data nextData();
    
    // TODO think about streaming big data
    //public InputStream getStream();

}
