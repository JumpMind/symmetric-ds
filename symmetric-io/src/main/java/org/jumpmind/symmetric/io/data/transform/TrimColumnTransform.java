package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;

public class TrimColumnTransform implements ISingleNewAndOldValueColumnTransform, IBuiltInExtensionPoint {

    public static final String NAME = "trim";

    public String getName() {
        return NAME;
    }

    public boolean isExtractColumnTransform() {
        return true;
    }
    
    public boolean isLoadColumnTransform() {
        return true;
    }

    public NewAndOldValue transform(IDatabasePlatform platform, DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
                    throws IgnoreColumnException, IgnoreRowException {
        if (newValue != null) {
            newValue = newValue.trim();
        }
        if (oldValue != null) {
            oldValue = oldValue.trim();
        }
        return new NewAndOldValue(newValue, oldValue);
    }

}