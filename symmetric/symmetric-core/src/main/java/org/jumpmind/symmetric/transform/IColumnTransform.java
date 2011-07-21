package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.IExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;

public interface IColumnTransform extends IExtensionPoint {

    public String getName();

    public String transform(IDataLoaderContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue)
            throws IgnoreColumnException, IgnoreRowException;

}
