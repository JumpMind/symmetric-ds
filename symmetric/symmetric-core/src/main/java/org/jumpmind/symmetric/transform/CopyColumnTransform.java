package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;

public class CopyColumnTransform implements IColumnTransform, IBuiltInExtensionPoint {

    public final static String NAME = "copy";
    
    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(IDataLoaderContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) {
        return value;
    }

}
