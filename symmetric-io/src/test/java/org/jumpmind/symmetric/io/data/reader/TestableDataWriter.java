package org.jumpmind.symmetric.io.data.reader;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

public class TestableDataWriter implements IDataWriter {

    List<CsvData> datas = new ArrayList<CsvData>();
    
    Table lastTableRead;
    
    public List<CsvData> getDatas() {
        return datas;
    }
   
    public Table getLastTableRead() {
        return lastTableRead;
    }
    
    @Override
    public void open(DataContext context) {
    }

    @Override
    public void close() {
    }

    @Override
    public Map<Batch, Statistics> getStatistics() {
        return null;
    }

    @Override
    public void start(Batch batch) {
    }

    @Override
    public boolean start(Table table) {
        lastTableRead = table;
        return true;
    }

    @Override
    public void write(CsvData data) {
        datas.add(data);
    }

    @Override
    public void end(Table table) {
    }

    @Override
    public void end(Batch batch, boolean inError) {
    }

}
