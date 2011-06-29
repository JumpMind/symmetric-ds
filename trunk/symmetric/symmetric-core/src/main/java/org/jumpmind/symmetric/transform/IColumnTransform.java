package org.jumpmind.symmetric.transform;

import org.jumpmind.symmetric.ext.IExtensionPoint;

public interface IColumnTransform extends IExtensionPoint {

    public String transform(TransformColumn column, TransformedData data,
            String value, String oldValue) throws IgnoreColumnException, IgnoreRowException;

}
