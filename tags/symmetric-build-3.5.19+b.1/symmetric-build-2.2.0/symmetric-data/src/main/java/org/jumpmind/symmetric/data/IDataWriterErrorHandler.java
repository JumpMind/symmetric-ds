package org.jumpmind.symmetric.data;

import org.jumpmind.symmetric.model.Batch;
import org.jumpmind.symmetric.model.Data;

public interface IDataWriterErrorHandler {

    public boolean handleWriteError(Exception error, Batch batch, Data data, int dataRow);    
    
}
