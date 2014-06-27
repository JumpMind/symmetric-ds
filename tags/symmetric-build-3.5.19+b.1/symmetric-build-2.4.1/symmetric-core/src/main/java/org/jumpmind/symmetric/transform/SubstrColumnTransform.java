package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

public class SubstrColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected IDbDialect dbDialect;
    
    public static final String NAME = "substr";

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        if (StringUtils.isNotBlank(value)) {
            String expression = column.getTransformExpression();
            if (StringUtils.isNotBlank(expression)) {
                String[] tokens = expression.split(",");
                if (tokens.length == 1) {
                    int index = Integer.parseInt(tokens[0]);
                    if (value.length() > index) {
                        return value.substring(index);
                    } else {
                        return "";
                    }
                } else if (tokens.length > 1) {
                    int beginIndex = Integer.parseInt(tokens[0]);
                    int endIndex = Integer.parseInt(tokens[1]);
                    if (value.length() > endIndex && endIndex > beginIndex) {
                        return value.substring(beginIndex, endIndex);
                    } else if (value.length() > beginIndex) {
                        return value.substring(beginIndex);
                    } else {
                        return "";
                    }
                }
            }
        }
        return value;
    }

    protected String[] prepend(String v, String[] array) {
        String[] dest = new String[array.length + 1];
        dest[0] = v;
        for (int i = 0; i < array.length; i++) {
            dest[i + 1] = array[i];
        }
        return dest;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

}
