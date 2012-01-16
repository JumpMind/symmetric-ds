package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class IdentityColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    public static final String NAME = "identity";

    public boolean isAutoRegister() {
        return true;
    }

    public String getName() {
        return NAME;
    }
    
    
    public boolean isExtractColumnTransform() {
        return false;
    }
    
    public boolean isLoadColumnTransform() {
        return true;
    }

    public String transform(IDatabasePlatform platform, DataContext<? extends IDataReader, ? extends IDataWriter> context, TransformColumn column,
            TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
            throws IgnoreColumnException, IgnoreRowException {  
        if (log.isDebugEnabled()) {
            log.debug("The %s transform requires a generated identity column.  This was configured using the %s target column.", data.getTransformation().getTransformId(), column.getTargetColumnName());
        }
        data.setGeneratedIdentityNeeded(true);
        throw new IgnoreColumnException();
    }

}