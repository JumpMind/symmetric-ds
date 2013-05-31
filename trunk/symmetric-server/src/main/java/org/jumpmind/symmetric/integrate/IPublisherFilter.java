package org.jumpmind.symmetric.integrate;

import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;

public interface IPublisherFilter extends IDatabaseWriterFilter {

    public void setPublisher(IPublisher publisher);
    
}