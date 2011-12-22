package org.jumpmind.symmetric.io.data;

import java.util.Map;

import org.jumpmind.util.Statistics;

public interface IDataResource {

    public <R extends IDataReader, W extends IDataWriter> void open(DataContext<R, W> context);

    public void close();

    public Map<Batch, Statistics> getStatistics();

}
