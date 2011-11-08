package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

public class CopyColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public final static String NAME = "copy";
    
    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) {
        return value;
    }

}
