package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.apache.commons.lang.time.DateFormatUtils;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;

public class VariableColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "variable";
    
    final String SOURCE_NODE_KEY = String.format("%d.SourceNode", hashCode());
    
    protected static final String TS_PATTERN = "yyyy-MM-dd HH:mm:ss.SSS";
    
    protected static final String DATE_PATTERN = "yyyy-MM-dd";
    
    protected static final String OPTION_TIMESTAMP = "system_timestamp";
    
    protected static final String OPTION_DATE = "system_date";
    
    protected static final String OPTION_EXTERNAL_ID = "source_external_id";
    
    private static final String[] OPTIONS = new String[] {OPTION_TIMESTAMP, OPTION_DATE, OPTION_EXTERNAL_ID};
    
    private INodeService nodeService;
    
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

    public String transform(ICacheContext context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String value, String oldValue) throws IgnoreColumnException,
            IgnoreRowException {
        String varName = column.getTransformExpression();
        if (varName != null) {
            if (varName.equalsIgnoreCase(OPTION_TIMESTAMP)) {
                return DateFormatUtils.format(System.currentTimeMillis(), TS_PATTERN);
            } else  if (varName.equalsIgnoreCase(OPTION_DATE)) {
                return DateFormatUtils.format(System.currentTimeMillis(), DATE_PATTERN);
            } else  if (varName.equalsIgnoreCase(OPTION_EXTERNAL_ID)) {
                Node sourceNode = (Node) context.getContextCache().get(SOURCE_NODE_KEY);
                if (sourceNode == null) {
                    sourceNode = nodeService.findNode(context.getSourceNodeId());
                    context.getContextCache().put(SOURCE_NODE_KEY, sourceNode);
                }
                return sourceNode != null ? sourceNode.getExternalId() : null;
            }
        }
        return null;
    }    

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }
}