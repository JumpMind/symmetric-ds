package org.jumpmind.symmetric.core.process;

import org.jumpmind.symmetric.core.model.Data;

public interface IDataWriter<T extends DataContext> {

    public void open(T context);

    public void startBatch(T context);

    public boolean switchTables(T context);

    public void writeData(Data data, T context);

    public void finishBatch(T context);

    public void close(T context);

    public T createDataContext();

}
