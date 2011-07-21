package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;

public class ConstantColumnTransform implements IColumnTransform, IBuiltInExtensionPoint {

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return "const";
    }

    public String transform(IDataLoaderContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        return column.getTransformExpression();
    }

}
