package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

public class VariableColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "variable";
    
    protected static final String DATE_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    
    protected static final String OPTION_TIMESTAMP = "system_timestamp";
    
    protected static final String OPTION_IDENTITY = "use_identity";
    
    private static final String[] OPTIONS = new String[] {OPTION_TIMESTAMP, OPTION_IDENTITY};
    
    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }
    
    public static String[] getOptions() {
        return OPTIONS;
    }

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        String varName = column.getTransformExpression();
        if (varName != null) {
            if (varName.equalsIgnoreCase(OPTION_TIMESTAMP)) {
                return DateFormatUtils.format(System.currentTimeMillis(), DATE_PATTERN);
            } else if (varName.equalsIgnoreCase(OPTION_IDENTITY)) {
                context.getContextCache().put(OPTION_IDENTITY, Boolean.TRUE);
                throw new IgnoreColumnException();
            }
        }
        return null;
    }

}