package org.jumpmind.symmetric.transform;

import org.jumpmind.symmetric.ext.IExtensionPoint;

public interface ITransform extends IExtensionPoint {        
    
    public String transform(TransformColumn column, String originalValue);        

}
