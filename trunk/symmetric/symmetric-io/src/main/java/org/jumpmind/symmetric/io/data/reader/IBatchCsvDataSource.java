package org.jumpmind.symmetric.io.data.reader;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;

public interface IBatchCsvDataSource {
    
    public Batch getBatch();
    
    /**
     * Return the table for the last {@link CsvData} retrieved by {@link #next()}
     */
    public Table getTable();
    
    public CsvData next();
    
    public boolean requiresLobsSelectedFromSource();
    
    public void close();

}
