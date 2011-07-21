package org.jumpmind.symmetric.transform;

import org.jumpmind.symmetric.load.IDataLoaderContext;

public class CopyColumnTransform implements IColumnTransform {

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return "copy";
    }

    public String transform(IDataLoaderContext context, TransformColumn column,
            TransformedData data, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        return value;
    }

}
