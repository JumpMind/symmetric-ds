package org.jumpmind.symmetric.data.csv;

import org.jumpmind.symmetric.data.DataContext;
import org.jumpmind.symmetric.data.IDataReader;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.model.Batch;
import org.jumpmind.symmetric.model.Data;

public class CsvDataReader implements IDataReader<DataContext> {

    @Override
    public void open(DataContext context) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public Batch nextBatch(DataContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Table nextTable(DataContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public Data nextData(DataContext context) {
        // TODO Auto-generated method stub
        return null;
    }

    @Override
    public void close(DataContext context) {
        // TODO Auto-generated method stub
        
    }

    @Override
    public DataContext createDataContext() {
        // TODO Auto-generated method stub
        return null;
    }

    
}
