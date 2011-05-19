package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

public interface IDataWriter<T extends DataContext> {

    public void open(T context);

    public void startBatch(Batch batch);

    public boolean switchTables(Table table);

    public void writeData(Data data);

    public void finishBatch(Batch batch);

    public void close();

    public T createDataContext();

}
