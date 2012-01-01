package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public interface IColumnTransform<T> extends IExtensionPoint {

    public String getName();

    public T transform(IDatabasePlatform platform, DataContext<? extends IDataReader, ? extends IDataWriter> context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
            throws IgnoreColumnException, IgnoreRowException;

    public boolean isExtractColumnTransform();

    public boolean isLoadColumnTransform();

}
