package org.jumpmind.symmetric.data;

import org.jumpmind.symmetric.common.BinaryEncoding;
import org.jumpmind.symmetric.model.Data;

public interface IDataWriter<T extends DataContext> {

    public void open(String nodeId, BinaryEncoding encoding, T context);

    public void startBatch(T context);

    public void switchTables(T context);

    public void writeData(Data data, T context);

    public void finishBatch(T context);

    public void close(T context);

    public T createDataContext();

}
