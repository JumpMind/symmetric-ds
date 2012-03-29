package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataWriter extends IDataResource {

    public void startBatch(Batch batch);

    public boolean writeTable(Table table);

    public boolean writeData(Data data);
    
    // TODO
    // public void writeStream(Outputstream)

    public void finishBatch(Batch batch);

}
