package org.jumpmind.symmetric.data.process.csv;

import org.jumpmind.symmetric.data.model.Batch;
import org.jumpmind.symmetric.data.model.Data;
import org.jumpmind.symmetric.data.model.Table;
import org.jumpmind.symmetric.data.process.DataContext;
import org.jumpmind.symmetric.data.process.IDataReader;

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
