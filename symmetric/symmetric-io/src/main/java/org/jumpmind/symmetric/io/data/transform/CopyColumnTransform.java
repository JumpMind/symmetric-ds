package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;

public class CopyColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    public final static String NAME = "copy";
    
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
            TransformColumn column, TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue) {
        return newValue;
    }

}
