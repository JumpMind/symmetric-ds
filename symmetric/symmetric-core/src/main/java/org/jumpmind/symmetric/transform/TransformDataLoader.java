package org.jumpmind.symmetric.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.DataLoaderFilterAdapter;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.dao.DataIntegrityViolationException;

public class TransformDataLoader extends DataLoaderFilterAdapter implements IBuiltInExtensionPoint {

    protected final ILog log = LogFactory.getLog(getClass());

    protected final String CACHE_KEY = "CACHE_KEY_" + this.hashCode();

    private ITransformService transformService;

    private IDbDialect dbDialect;

    private IParameterService parameterService;
    
    private String tablePrefix;

    public TransformDataLoader() {
        super(true);
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        return !transform(DmlType.INSERT, context, context.getColumnNames(), columnValues, null,
                null);
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues,
            String[] keyValues) {
        return !transform(DmlType.UPDATE, context, context.getColumnNames(), columnValues,
                context.getKeyNames(), keyValues);
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        String[] columnNames = context.getKeyNames();
        String[] columnValues = keyValues;
        if (context.getOldData() != null) {
            columnNames = context.getColumnNames();
            columnValues = context.getOldData();
        }
        return !transform(DmlType.DELETE, context, columnNames, columnValues,
                context.getKeyNames(), keyValues);
    }

    protected boolean transform(DmlType dmlType, IDataLoaderContext context, String[] columnNames,
            String[] columnValues, String[] keyNames, String[] keyValues) {
        if (!context.getTableName().toLowerCase().startsWith(tablePrefix)) {
            List<TransformTable> transformationsToPerform = findTablesToTransform(context
                    .getTableTemplate().getFullyQualifiedTableName(true),
                    parameterService.getNodeGroupId());
            if (transformationsToPerform != null && transformationsToPerform.size() > 0) {
                Map<String, String> sourceValues = toMap(columnNames, columnValues);
                Map<String, String> oldSourceValues = toMap(columnNames, context.getOldData());
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

    protected boolean perform(IDataLoaderContext context, TransformedData data,
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

    protected TransformedData create(IDataLoaderContext context, DmlType dmlType,
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

    protected void transformColumn(IDataLoaderContext context, TransformedData data,
            TransformColumn transformColumn, Map<String, String> sourceValues,
            Map<String, String> oldSourceValues, boolean recordAsKey) throws IgnoreRowException {
        try {
            String value = transformColumn.getSourceColumnName() != null ? sourceValues
                    .get(transformColumn.getSourceColumnName()) : null;
            IColumnTransform transform = transformService.getColumnTransforms().get(
                    transformColumn.getTransformType());
            if (transform != null) {
                String oldValue = oldSourceValues.get(transformColumn.getSourceColumnName());
                value = transform.transform(context, transformColumn, data, sourceValues, value, oldValue);
            }
            data.put(transformColumn, value, recordAsKey);
        } catch (IgnoreColumnException e) {
            // Do nothing. We are suppose to ignore the column
        }
    }

    public void apply(IDataLoaderContext context, TransformedData data) {
        TableTemplate tableTemplate = new TableTemplate(context.getJdbcTemplate(), dbDialect,
                data.getTableName(), null, false, data.getSchemaName(), data.getCatalogName());
        tableTemplate.setColumnNames(data.getColumnNames());
        tableTemplate.setKeyNames(data.getKeyNames());
        // TODO Need more advanced fallback logic? Support typical
        // symmetric fallback/recovery settings?
        switch (data.getTargetDmlType()) {
        case INSERT:
            try {
                tableTemplate.insert(context, data.getColumnValues());
            } catch (DataIntegrityViolationException ex) {
                data.setTargetDmlType(DmlType.UPDATE);
                tableTemplate.setColumnNames(data.getColumnNames());
                tableTemplate.setKeyNames(data.getKeyNames());
                tableTemplate.update(context, data.getColumnValues(), data.getKeyValues());
            }
            break;
        case UPDATE:
            if (0 == tableTemplate.update(context, data.getColumnValues(), data.getKeyValues())
                    && (data.getSourceDmlType() != DmlType.DELETE)) {
                data.setTargetDmlType(DmlType.INSERT);
                tableTemplate.setColumnNames(data.getColumnNames());
                tableTemplate.setKeyNames(data.getKeyNames());
                tableTemplate.insert(context, data.getColumnValues());
            }
            break;
        case DELETE:
            tableTemplate.delete(context, data.getKeyValues());
            break;
        }

    }

    protected List<TransformTable> findTablesToTransform(String fullyQualifiedSourceTableName,
            String targetNodeId) {
        Map<String, List<TransformTable>> transformMap = transformService.findTransformsFor(
                targetNodeId, true);
        List<TransformTable> transforms = transformMap != null ? transformMap
                .get(fullyQualifiedSourceTableName) : null;
        return transforms;
    }

    public void setTransformService(ITransformService transformService) {
        this.transformService = transformService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
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

    @Override
    public boolean isHandlingMissingTable(IDataLoaderContext context) {
        List<TransformTable> transformationsToPerform = findTablesToTransform(context
                .getTableTemplate().getFullyQualifiedTableName(true), parameterService.getNodeGroupId());
        return transformationsToPerform != null && transformationsToPerform.size() > 0;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }
    
    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }
}
