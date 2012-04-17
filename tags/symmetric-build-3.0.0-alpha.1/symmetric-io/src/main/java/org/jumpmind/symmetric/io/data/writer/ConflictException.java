package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.CsvData;

public class ConflictException extends RuntimeException {

    private static final long serialVersionUID = 1L;
    
    protected CsvData data;
    
    protected Table table;
    
    protected boolean fallbackOperationFailed = false;

    public ConflictException(CsvData data, Table table, boolean fallbackOperationFailed) {
        this.data = data;
        this.table = table;
        this.fallbackOperationFailed = fallbackOperationFailed;
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

}
