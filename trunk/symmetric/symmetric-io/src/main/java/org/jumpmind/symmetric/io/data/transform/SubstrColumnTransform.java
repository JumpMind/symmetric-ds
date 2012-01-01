package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class SubstrColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "substr";

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }
    
    
    public boolean isExtractColumnTransform() {
        return true;
    }
    
    public boolean isLoadColumnTransform() {
        return true;
    }

    public String transform(IDatabasePlatform platform, DataContext<? extends IDataReader, ? extends IDataWriter> context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        if (StringUtils.isNotBlank(newValue)) {
            String expression = column.getTransformExpression();
            if (StringUtils.isNotBlank(expression)) {
                String[] tokens = expression.split(",");
                if (tokens.length == 1) {
                    int index = Integer.parseInt(tokens[0]);
                    if (newValue.length() > index) {
                        return newValue.substring(index);
                    } else {
                        return "";
                    }
                } else if (tokens.length > 1) {
                    int beginIndex = Integer.parseInt(tokens[0]);
                    int endIndex = Integer.parseInt(tokens[1]);
                    if (newValue.length() > endIndex && endIndex > beginIndex) {
                        return newValue.substring(beginIndex, endIndex);
                    } else if (newValue.length() > beginIndex) {
                        return newValue.substring(beginIndex);
                    } else {
                        return "";
                    }
                }
            }
        }
        return newValue;
    }

    protected String[] prepend(String v, String[] array) {
        String[] dest = new String[array.length + 1];
        dest[0] = v;
        for (int i = 0; i < array.length; i++) {
            dest[i + 1] = array[i];
        }
        return dest;
    }

}
