package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataWriter {

    public void open(DataContext context);

    public void startBatch(Batch batch);

    public boolean writeTable(Table table);

    public void writeData(Data data);

    public void finishBatch(Batch batch);

    public void close();

}
