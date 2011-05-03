package org.jumpmind.symmetric.core.process.sql;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.IDataReader;

public class SqlDataReader implements IDataReader<SqlDataContext> {

    
    public void close(SqlDataContext context) {
    }
    
    
    public SqlDataContext createDataContext() {

        return null;
    }
    
    
    public Batch nextBatch(SqlDataContext context) {

        return null;
    }
    
    
    public Data nextData(SqlDataContext context) {

        return null;
    }
    
    
    public Table nextTable(SqlDataContext context) {

        return null;
    }
    
    
    public void open(SqlDataContext context) {
        
    }
}
