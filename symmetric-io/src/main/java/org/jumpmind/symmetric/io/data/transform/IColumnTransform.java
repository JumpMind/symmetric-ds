package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;

public interface IColumnTransform<T> extends IExtensionPoint {

    public String getName();

    public T transform(IDatabasePlatform platform, DataContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
            throws IgnoreColumnException, IgnoreRowException;

    public boolean isExtractColumnTransform();

    public boolean isLoadColumnTransform();

}
