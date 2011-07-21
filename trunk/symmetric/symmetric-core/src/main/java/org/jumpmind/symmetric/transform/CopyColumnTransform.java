package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.load.IDataLoaderContext;

public class CopyColumnTransform implements IColumnTransform {

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return "copy";
    }

    public String transform(IDataLoaderContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) {
        return value;
    }

}
