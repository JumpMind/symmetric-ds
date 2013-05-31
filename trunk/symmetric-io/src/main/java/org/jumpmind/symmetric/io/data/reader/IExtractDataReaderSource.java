package org.jumpmind.symmetric.io.data.reader;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;

public interface IExtractDataReaderSource {
    
    public Batch getBatch();
    
    /**
     * Return the table with the catalog, schema, and table name of the target table for the last {@link CsvData} retrieved by {@link #next()}
     */
    public Table getTargetTable();
    
    /**
     * Return the table with the catalog, schema, and table name of the source table for the last {@link CsvData} retrieved by {@link #next()}
     */
    public Table getSourceTable();
    
    public CsvData next();
    
    public boolean requiresLobsSelectedFromSource();
    
    public void close();

}
