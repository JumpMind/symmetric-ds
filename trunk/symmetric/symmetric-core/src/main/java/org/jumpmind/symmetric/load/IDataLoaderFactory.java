package org.jumpmind.symmetric.load;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.IDataWriter;

public interface IDataLoaderFactory extends IExtensionPoint {
    
    public String getTypeName();
    
    public IDataWriter getDataWriter(String sourceNodeId, IDatabasePlatform platform);
    
    public boolean isPlatformSupported(IDatabasePlatform platform);

}
