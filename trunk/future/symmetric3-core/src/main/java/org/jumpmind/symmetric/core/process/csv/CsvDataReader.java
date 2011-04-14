package org.jumpmind.symmetric.core.process.csv;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IDataReader;

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
