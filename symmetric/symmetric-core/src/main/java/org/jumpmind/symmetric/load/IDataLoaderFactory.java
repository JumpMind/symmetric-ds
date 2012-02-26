package org.jumpmind.symmetric.load;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.IDatabaseWriterFilter;
import org.jumpmind.symmetric.io.data.writer.TransformWriter;

public interface IDataLoaderFactory extends IExtensionPoint {
    
    public String getTypeName();
    
    public IDataWriter getDataWriter(String sourceNodeId, IDatabasePlatform platform, TransformWriter transformWriter, IDatabaseWriterFilter[] filters);
    
    public boolean isPlatformSupported(IDatabasePlatform platform);

}
