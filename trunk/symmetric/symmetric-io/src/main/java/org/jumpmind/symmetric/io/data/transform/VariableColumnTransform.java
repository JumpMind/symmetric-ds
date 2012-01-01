package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class VariableColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "variable";

    final String SOURCE_NODE_KEY = String.format("%d.SourceNode", hashCode());

    protected static final String TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";

    protected static final String DATE_PATTERN = "yyyy-MM-dd";

    protected static final String OPTION_TIMESTAMP = "system_timestamp";

    protected static final String OPTION_DATE = "system_date";

    protected static final String OPTION_SOURCE_NODE_ID = "source_node_id";

    private static final String[] OPTIONS = new String[] { OPTION_TIMESTAMP, OPTION_DATE,
            OPTION_SOURCE_NODE_ID };

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

    public static String[] getOptions() {
        return OPTIONS;
    }

    public String transform(IDatabasePlatform platform,
            DataContext<? extends IDataReader, ? extends IDataWriter> context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {
        String varName = column.getTransformExpression();
        if (varName != null) {
            if (varName.equalsIgnoreCase(OPTION_TIMESTAMP)) {
                return DateFormatUtils.format(System.currentTimeMillis(), TS_PATTERN);
            } else if (varName.equalsIgnoreCase(OPTION_DATE)) {
                return DateFormatUtils.format(System.currentTimeMillis(), DATE_PATTERN);
            } else if (varName.equalsIgnoreCase(OPTION_SOURCE_NODE_ID)) {
                return context.getBatch().getSourceNodeId();
            }
        }
        return null;
    }

}