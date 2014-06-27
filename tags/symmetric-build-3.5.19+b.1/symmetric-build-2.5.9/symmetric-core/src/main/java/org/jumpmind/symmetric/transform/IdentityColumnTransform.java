package org.jumpmind.symmetric.transform;

import java.util.Map;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;

public class IdentityColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final ILog log = LogFactory.getLog(getClass());
    
    public static final String NAME = "identity";

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }

    public String transform(ICacheContext context, TransformColumn column, TransformedData data,
            Map<String, String> sourceValues, String value, String oldValue)
            throws IgnoreColumnException, IgnoreRowException {  
        if (log.isDebugEnabled()) {
            log.debug("TransformGeneratedIdentityNeeded", data.getTransformation().getTransformId(), column.getTargetColumnName());
        }
        data.setGeneratedIdentityNeeded(true);
        throw new IgnoreColumnException();
    }

}