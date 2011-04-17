package org.jumpmind.symmetric.jdbc.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.IDataReader;

public class JdbcDataReader implements IDataReader<JdbcDataContext> {

    @Override
    public void close(JdbcDataContext context) {
        // TODO Auto-generated method stub
        
    }
    
    @Override
    public JdbcDataContext createDataContext() {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Batch nextBatch(JdbcDataContext context) {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Data nextData(JdbcDataContext context) {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public Table nextTable(JdbcDataContext context) {
        // TODO Auto-generated method stub
        return null;
    }
    
    @Override
    public void open(JdbcDataContext context) {
        // TODO Auto-generated method stub
        
    }
}
