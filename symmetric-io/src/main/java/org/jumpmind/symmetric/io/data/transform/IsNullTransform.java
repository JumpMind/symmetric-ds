package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IsNullTransform implements ISingleNewAndOldValueColumnTransform, IBuiltInExtensionPoint {

	protected final Logger log = LoggerFactory.getLogger(getClass());

	public static final String NAME = "isNull";

	public String getName() {
		return NAME;
	}
	
	@Override
    public NewAndOldValue transform(IDatabasePlatform platform,
            DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {

	    String value = data.getSourceDmlType().equals(DataEventType.DELETE) ? oldValue : newValue;
	    
	    if (value == null) {
			String expression = column.getTransformExpression();
            if (StringUtils.isNotEmpty(expression)) {
                value = expression;
            }
		}

        if (data.getTargetDmlType().equals(DataEventType.DELETE)) {
            return new NewAndOldValue(null, value);
        } else {
            return new NewAndOldValue(value, null);
        }
	}

	@Override
	public boolean isExtractColumnTransform() {
		return true;
	}

	@Override
	public boolean isLoadColumnTransform() {
		return true;
	}

}
