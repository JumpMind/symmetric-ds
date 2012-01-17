package org.jumpmind.symmetric.io.data;

import java.util.Map;

import org.jumpmind.util.Statistics;

public interface IDataResource {

    public void open(DataContext context);

    public void close();

    public Map<Batch, Statistics> getStatistics();

}
