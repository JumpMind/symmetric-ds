package org.jumpmind.symmetric.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.service.IParameterService;

public abstract class AbstractTransformer {

    protected final ILog log = LogFactory.getLog(getClass());

    protected ITransformService transformService;

    protected IDbDialect dbDialect;

    protected IParameterService parameterService;

    protected String tablePrefix;

    protected boolean transform(DmlType dmlType, ICacheContext context, String catalogName,
            String schemaName, String tableName, String[] columnNames, String[] columnValues,
            String[] keyNames, String[] keyValues, String[] oldData) {
        if (!tableName.toLowerCase().startsWith(tablePrefix)) {
            List<TransformTable> transformationsToPerform = findTablesToTransform(
                    getFullyQualifiedTableName(catalogName, schemaName, tableName),
                    parameterService.getNodeGroupId());
            if (transformationsToPerform != null && transformationsToPerform.size() > 0) {
                Map<String, String> sourceValues = toMap(columnNames, columnValues);
                Map<String, String> oldSourceValues = toMap(columnNames, oldData);
                Map<String, String> sourceKeyValues = sourceValues;
                if (keyNames != null) {
                    sourceKeyValues = toMap(keyNames, keyValues);
                }
                try {
                    for (TransformTable transformation : transformationsToPerform) {
                        TransformedData targetData = create(context, dmlType, transformation,
                                sourceKeyValues, oldSourceValues);
                        if (perform(context, targetData, transformation, sourceValues,
                                oldSourceValues)) {
                            apply(context, targetData);
                        }
                    }
                } catch (IgnoreRowException e) {
                    // Do nothing. We are suppose to ignore this row.
                }
                return true;
            }
        }
        return false;
    }

    protected boolean perform(ICacheContext context, TransformedData data,
            TransformTable transformation, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException {
        boolean persistData = false;

        for (TransformColumn transformColumn : transformation.getTransformColumns()) {
            if (transformColumn.getSourceColumnName() == null
                    || sourceValues.containsKey(transformColumn.getSourceColumnName())) {
                transformColumn(context, data, transformColumn, sourceValues, oldSourceValues,
                        false);
            } else {
                log.warn("TransformSourceColumnNotFound", transformColumn.getSourceColumnName(),
                        transformation.getTransformId());
            }
        }

        if (data.getTargetDmlType() != DmlType.DELETE) {
            if (data.getTargetDmlType() == DmlType.INSERT && transformation.isUpdateFirst()) {
                data.setTargetDmlType(DmlType.UPDATE);
            }
            persistData = true;
        } else {
            // handle the delete action
            DeleteAction deleteAction = transformation.getDeleteAction();
            switch (deleteAction) {
            case NONE:
                break;
            case DEL_ROW:
                data.setTargetDmlType(DmlType.DELETE);
                persistData = true;
                break;
            case UPDATE_COL:
                data.setTargetDmlType(DmlType.UPDATE);
                persistData = true;
                break;
            }
        }
        return persistData;
    }

    abstract protected void apply(ICacheContext context, TransformedData data);

    protected TransformedData create(ICacheContext context, DmlType dmlType,
            TransformTable transformation, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues) throws IgnoreRowException {
        TransformedData row = new TransformedData(transformation, dmlType);
        List<TransformColumn> columns = transformation.getPrimaryKeyColumns();
        if (columns.size() == 0) {
            log.error("TransformNoPrimaryKeyDefined", transformation.getTransformId());
        } else {
            for (TransformColumn transformColumn : columns) {
                transformColumn(context, row, transformColumn, sourceValues, oldSourceValues, true);
            }
        }
        return row;
    }

    protected void transformColumn(ICacheContext context, TransformedData data,
            TransformColumn transformColumn, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues, boolean recordAsKey) throws IgnoreRowException {
        try {
            String value = transformColumn.getSourceColumnName() != null ? sourceValues
                    .get(transformColumn.getSourceColumnName()) : null;
            IColumnTransform transform = transformService.getColumnTransforms().get(
                    transformColumn.getTransformType());
            if (transform != null) {
                String oldValue = oldSourceValues.get(transformColumn.getSourceColumnName());
                value = transform.transform(context, transformColumn, data, sourceValues, value,
                        oldValue);
            }
            data.put(transformColumn, value, recordAsKey);
        } catch (IgnoreColumnException e) {
            // Do nothing. We are suppose to ignore the column
        }
    }

    protected String getFullyQualifiedTableName(String catalogName, String schemaName,
            String tableName) {
        if (!StringUtils.isBlank(schemaName)) {
            tableName = schemaName + "." + tableName;
        }
        if (!StringUtils.isBlank(catalogName)) {
            tableName = catalogName + "." + tableName;
        }
        return tableName;
    }

    protected List<TransformTable> findTablesToTransform(String fullyQualifiedSourceTableName,
            String targetNodeId) {
        Map<String, List<TransformTable>> transformMap = transformService.findTransformsFor(
                targetNodeId, true);
        List<TransformTable> transforms = transformMap != null ? transformMap
                .get(fullyQualifiedSourceTableName) : null;
        return transforms;
    }

    protected Map<String, String> toMap(String[] columnNames, String[] columnValues) {
        if (columnValues != null) {
            Map<String, String> map = new HashMap<String, String>(columnNames.length);
            for (int i = 0; i < columnNames.length; i++) {
                map.put(columnNames[i], columnValues[i]);
            }
            return map;
        } else {
            return new HashMap<String, String>(0);
        }
    }

    public void setTransformService(ITransformService transformService) {
        this.transformService = transformService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public boolean isAutoRegister() {
        return true;
    }

}
