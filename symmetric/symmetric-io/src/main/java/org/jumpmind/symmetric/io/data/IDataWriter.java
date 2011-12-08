package org.jumpmind.symmetric.io.data;

import org.jumpmind.db.model.Table;


public interface IDataWriter extends IDataResource {

    public void startBatch(Batch batch);

    public boolean writeTable(Table table);

    public boolean writeData(CsvData data);
    
    // TODO
    // public void writeStream(Outputstream)

    public void finishBatch(Batch batch);

}
