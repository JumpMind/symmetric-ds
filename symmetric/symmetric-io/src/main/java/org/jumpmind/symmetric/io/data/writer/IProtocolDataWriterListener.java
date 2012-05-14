package org.jumpmind.symmetric.io.data.writer;

import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.stage.IStagedResource;

public interface IProtocolDataWriterListener {
    
    public void start(DataContext ctx, Batch batch);
    
    public void end(DataContext ctx, Batch batch, IStagedResource resource);

}
