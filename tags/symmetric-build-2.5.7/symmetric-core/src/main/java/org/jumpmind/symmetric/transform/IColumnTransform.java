package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.ext.IExtensionPoint;

public interface IColumnTransform<T> extends IExtensionPoint {

    public String getName();

    public T transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue)
            throws IgnoreColumnException, IgnoreRowException;

}
