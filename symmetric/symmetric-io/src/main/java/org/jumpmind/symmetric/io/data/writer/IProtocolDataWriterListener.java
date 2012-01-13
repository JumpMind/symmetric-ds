package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.symmetric.io.IoResource;
import org.jumpmind.symmetric.io.data.Batch;

public interface IProtocolDataWriterListener {
    
    public void start(Batch batch);
    
    public void end(Batch batch, IoResource resource);

}
