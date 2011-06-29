package org.jumpmind.symmetric.transform;

public class AdditiveColumnTransform implements IColumnTransform {

    public boolean isAutoRegister() {
        return true;
    }

    public String transform(TransformColumn column, TransformedData data, String value,
            String oldValue) throws IgnoreColumnException, IgnoreRowException {
        return null;
    }

}
