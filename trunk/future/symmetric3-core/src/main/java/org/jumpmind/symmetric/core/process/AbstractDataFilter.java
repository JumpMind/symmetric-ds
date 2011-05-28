package org.jumpmind.symmetric.core.process;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.Table;

abstract public class AbstractDataFilter implements IDataWriter {

    protected final Log log = LogFactory.getLog(getClass());

    protected List<IColumnFilter> columnFilters;

    protected List<IDataFilter> dataFilters;
    
    protected static List<IDataFilter> toList(IDataFilter filter) {
        List<IDataFilter> list = new ArrayList<IDataFilter>(1);
        list.add(filter);
        return list;
    }
    
    protected boolean filterData(Data data, Batch batch, Table targetTable, DataContext ctx) {
        boolean continueToLoad = true;
        if (dataFilters != null) {
            batch.startTimer();
            for (IDataFilter filter : dataFilters) {
                continueToLoad &= filter.filter(ctx, targetTable, data);
            }
            batch.incrementFilterMillis(batch.endTimer());
        }
        return continueToLoad;
    }
}
