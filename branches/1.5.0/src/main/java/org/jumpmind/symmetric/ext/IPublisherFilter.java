package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IDataLoaderFilter;

public interface IPublisherFilter extends IDataLoaderFilter, IBatchListener {

    public void setPublisher(IPublisher publisher);
    
}
