package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.writer.DatabaseWriter.LoadStatus;

public class ConflictException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    protected CsvData data;
    
    protected Table table;
    
    protected boolean fallbackOperationFailed = false;
    
    protected LoadStatus loadStatus;

    public ConflictException(CsvData data, Table table, LoadStatus loadStatus, boolean fallbackOperationFailed) {
        this.data = data;
        this.table = table;
        this.fallbackOperationFailed = fallbackOperationFailed;
        this.loadStatus = loadStatus;
    }
    
    public CsvData getData() {
        return data;
    }
    
    public Table getTable() {
        return table;
    }
    
    public boolean isFallbackOperationFailed() {
        return fallbackOperationFailed;
    }
    
    public LoadStatus getLoadStatus() {
        return loadStatus;
    }

}
