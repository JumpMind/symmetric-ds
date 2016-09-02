package org.jumpmind.symmetric.io.data.transform;

import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IsBlankTransform implements ISingleNewAndOldValueColumnTransform, IBuiltInExtensionPoint {
	
	protected final Logger log = LoggerFactory.getLogger(getClass());

	public static final String NAME = "isBlank";

	public String getName() {
		return NAME;
	}

	public boolean isExtractColumnTransform() {
		return true;
	}

	public boolean isLoadColumnTransform() {
		return true;
	}
	
	@Override
	public NewAndOldValue transform(IDatabasePlatform platform, DataContext context, TransformColumn column,
			TransformedData data, Map<String, String> sourceValues, String newValue, String oldValue)
			throws IgnoreColumnException, IgnoreRowException {
		
		NewAndOldValue result = new NewAndOldValue(newValue, oldValue);
		if(StringUtils.isBlank(newValue)) { 
			String expression = column.getTransformExpression();
            if (StringUtils.isEmpty(expression)) {
            	expression = null;
            }
            result = new NewAndOldValue(expression, oldValue); 
        }
		return result;
	}

}
