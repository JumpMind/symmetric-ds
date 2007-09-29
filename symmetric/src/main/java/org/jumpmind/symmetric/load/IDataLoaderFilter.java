package org.jumpmind.symmetric.load;

import org.springframework.transaction.annotation.Transactional;

@Transactional
public interface IDataLoaderFilter {

    public void filterInsert(IDataLoaderContext context, String[] columnValues);
    
    public void filterUpdate(IDataLoaderContext context, String[] columnValues, String[] keyValues);
    
    public void filterDelete(IDataLoaderContext context, String[] keyValues);

}
