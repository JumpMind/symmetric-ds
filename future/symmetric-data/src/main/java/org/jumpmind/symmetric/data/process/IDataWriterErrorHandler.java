package org.jumpmind.symmetric.data.process;

import org.jumpmind.symmetric.data.model.Batch;
import org.jumpmind.symmetric.data.model.Data;

public interface IDataWriterErrorHandler {

    public boolean handleWriteError(Exception error, Batch batch, Data data, int dataRow);

}
