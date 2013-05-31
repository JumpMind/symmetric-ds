package org.jumpmind.symmetric.io.data.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.extension.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.io.data.DataContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LookupColumnTransform implements ISingleValueColumnTransform, IBuiltInExtensionPoint {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    public static final String NAME = "lookup";

    protected static final StringMapper lookupColumnRowMapper = new StringMapper();

    public String getName() {
        return NAME;
    }

    public boolean isExtractColumnTransform() {
        return true;
    }

    public boolean isLoadColumnTransform() {
        return true;
    }

    public String transform(IDatabasePlatform platform, DataContext context,
            TransformColumn column, TransformedData data, Map<String, String> sourceValues,
            String newValue, String oldValue) throws IgnoreColumnException, IgnoreRowException {
        String sql = column.getTransformExpression();
        String lookupValue = null;

        if (StringUtils.isNotBlank(sql)) {
            ISqlTransaction transaction = context.findTransaction();
            List<String> values = null;
            if (transaction != null) {
                values = transaction.query(sql, lookupColumnRowMapper, new HashMap<String, Object>(
                        sourceValues));
            } else {
                values = platform.getSqlTemplate().query(sql, lookupColumnRowMapper,
                        new HashMap<String, Object>(sourceValues));
            }

            int rowCount = values.size();

            if (rowCount == 1) {
                lookupValue = values.get(0);
            } else if (rowCount > 1) {
                lookupValue = values.get(0);
                log.warn(
                        "Expected a single row, but returned multiple rows from lookup for target column {} on transform {} ",
                        column.getTargetColumnName(), column.getTransformId());
            } else if (values.size() == 0) {
                log.warn(
                        "Expected a single row, but returned no rows from lookup for target column {} on transform {}",
                        column.getTargetColumnName(), column.getTransformId());
            }
        } else {
            log.warn(
                    "Expected SQL expression for lookup transform, but no expression was found for target column {} on transform {}",
                    column.getTargetColumnName(), column.getTransformId());
        }
        return lookupValue;
    }

}
