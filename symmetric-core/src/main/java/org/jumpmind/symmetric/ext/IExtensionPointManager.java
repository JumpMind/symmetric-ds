package org.jumpmind.symmetric.ext;

import java.util.List;

import org.jumpmind.extension.IExtensionPoint;

/**
 * An API that is responsible for finding and registering available
 * extension points.
 */
public interface IExtensionPointManager {

    public void register();
    
    public List<ExtensionPointMetaData> getExtensionPoints();
    
    public <T extends IExtensionPoint> T getExtensionPoint(String name);
    
}