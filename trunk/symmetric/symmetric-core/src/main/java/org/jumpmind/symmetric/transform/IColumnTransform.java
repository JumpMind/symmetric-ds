package org.jumpmind.symmetric.transform;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;

public interface IColumnTransform extends IExtensionPoint {
    
    public String getName();

    public String transform(IDataLoaderContext context, TransformColumn column, TransformedData data,
            String value, String oldValue) throws IgnoreColumnException, IgnoreRowException;

}
