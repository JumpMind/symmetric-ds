package org.jumpmind.symmetric.ext;

import org.jumpmind.symmetric.load.IDataLoaderContext;

public interface IPublisher {
    public void publish(IDataLoaderContext context, String text);
}
