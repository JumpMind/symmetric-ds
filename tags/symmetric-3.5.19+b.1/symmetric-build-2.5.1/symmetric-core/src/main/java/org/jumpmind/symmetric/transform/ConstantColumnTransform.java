package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

public class ConstantColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "const";
    
    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        return column.getTransformExpression();
    }

}
