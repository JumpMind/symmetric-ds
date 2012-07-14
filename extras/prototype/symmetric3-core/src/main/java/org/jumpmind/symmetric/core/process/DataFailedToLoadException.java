package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.db.SqlException;
import org.jumpmind.symmetric.core.model.Data;

public class DataFailedToLoadException extends SqlException {

    private static final long serialVersionUID = 1L;

    private Data failedData;

    public DataFailedToLoadException(Data failedData, String message) {
        this(failedData, message, null);
    }

    public DataFailedToLoadException(Data failedData, Exception ex) {
        this(failedData, ex.getMessage(), ex);
    }

    public DataFailedToLoadException(Data failedData, String message, Exception ex) {
        super(message, ex);
        this.failedData = failedData;
    }

    public Data getFailedData() {
        return failedData;
    }
}
