package org.jumpmind.symmetric.io.data.transform;

import java.util.List;
import java.util.Map;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;

public class MultiplierColumnTransform implements IMultipleValueColumnTransform,
        IBuiltInExtensionPoint {

    public static final String NAME = "multiply";

    protected static final StringMapper rowMapper = new StringMapper();

    public String getName() {
        return NAME;
    }

    public boolean isExtractColumnTransform() {
        return true;
    }

    public boolean isLoadColumnTransform() {
        return true;
    }

    public List<String> transform(IDatabasePlatform platform,
            DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {
        return platform.getSqlTemplate().query(column.getTransformExpression(), rowMapper,
                sourceValues);
    }

}
