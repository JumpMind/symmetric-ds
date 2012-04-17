package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.stage.IStagedResource;

public interface IProtocolDataWriterListener {
    
    public void start(Batch batch);
    
    public void end(Batch batch, IStagedResource resource);

}
