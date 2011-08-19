package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

public class VariableColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "variable";
    
    protected static final String TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    
    protected static final String DATE_PATTERN = "yyyy-MM-dd";
    
    protected static final String OPTION_TIMESTAMP = "system_timestamp";
    
    protected static final String OPTION_DATE = "system_date";
    
    private static final String[] OPTIONS = new String[] {OPTION_TIMESTAMP, OPTION_DATE};
    
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
                return DateFormatUtils.format(System.currentTimeMillis(), TS_PATTERN);
            } else  if (varName.equalsIgnoreCase(OPTION_DATE)) {
                return DateFormatUtils.format(System.currentTimeMillis(), DATE_PATTERN);
            }
        }
        return null;
    }

}