package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;

public interface IDataWriterErrorHandler {

    public boolean handleWriteError(Exception error, Batch batch, Data data, int dataRow);

}
