package org.jumpmind.symmetric.transform;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.h2.util.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.DataLoaderFilterAdapter;
import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.load.TableTemplate;
import org.springframework.dao.DataIntegrityViolationException;

public class TransformDataLoader extends DataLoaderFilterAdapter {

    protected final String CACHE_KEY = "CACHE_KEY_" + this.hashCode();

    private ITransformService transformService;

    private IDbDialect dbDialect;

    private Map<String, ITransform> transforms = new HashMap<String, ITransform>();

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
        return !transform(DmlType.DELETE, context, context.getKeyNames(), keyValues,
                context.getKeyNames(), keyValues);
    }

    protected boolean transform(DmlType dmlType, IDataLoaderContext context, String[] columnNames,
            String[] columnValues, String[] keyNames, String[] keyValues) {
        List<TransformTable> transformationsToPerform = findTablesToTransform(context
                .getTableTemplate().getFullyQualifiedTableName(), context.getTargetNodeId());
        if (transformationsToPerform != null && transformationsToPerform.size() > 0) {
            Map<String, String> originalValues = toMap(columnNames, columnValues);
            Map<String, String> originalkeyValues = originalValues;
            if (keyNames != null) {
                originalkeyValues = toMap(keyNames, keyValues);
            }
            for (TransformTable transformation : transformationsToPerform) {
                TransformedData targetData = create(dmlType, transformation, originalkeyValues);
                if (perform(targetData, transformation, originalValues)) {
                    apply(context, targetData);
                }
            }
            return true;
        } else {
            return false;
        }
    }

    protected boolean perform(TransformedData data, TransformTable transformation,
            Map<String, String> originalValues) {
        boolean persistData = false;
        if (data.getTargetDmlType() != DmlType.DELETE) {
            if (data.getTargetDmlType() == DmlType.INSERT && transformation.isUpdateFirst()) {
                data.setTargetDmlType(DmlType.UPDATE);
            }
            for (String columnName : originalValues.keySet()) {
                List<TransformColumn> transformColumns = transformation.getTransformColumnFor(columnName);
                for (TransformColumn transformColumn : transformColumns) {
                    transformColumn(data, transformColumn, originalValues, false);
                }
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
            case NULL_COL:
                data.setTargetDmlType(DmlType.UPDATE);
                persistData = true;
                break;
            }
        }
        return persistData;
    }

    protected TransformedData create(DmlType dmlType, TransformTable transformation,
            Map<String, String> originalValues) {
        TransformedData row = new TransformedData(transformation, dmlType);
        List<TransformColumn> columns = transformation.getPrimaryKeyColumns();
        for (TransformColumn transformColumn : columns) {
            transformColumn(row, transformColumn, originalValues, true);
        }
        return row;
    }

    protected void transformColumn(TransformedData data, TransformColumn transformColumn,
            Map<String, String> originalValues, boolean recordAsKey) {
        String value = originalValues.get(transformColumn.getSourceColumnName());
        if (transformColumn.getSourceColumnName().startsWith("\"")) {
            value = StringUtils.trim(transformColumn.getSourceColumnName(), true, true, "\"");
        }
        ITransform transform = transforms.get(transformColumn.getTransformType());
        if (transform != null) {
            value = transform.transform(transformColumn, value);
        }
        data.put(transformColumn, value, recordAsKey);
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
                tableTemplate.update(context, data.getColumnValues(), data.getKeyValues());
            }
            break;
        case UPDATE:
            if (0 == tableTemplate.update(context, data.getColumnValues(), data.getKeyValues())) {
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
        List<TransformTable> transforms = transformMap.get(fullyQualifiedSourceTableName);
        return transforms;
    }

    public void setTransformService(ITransformService transformService) {
        this.transformService = transformService;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void addTransform(String name, ITransform transform) {
        transforms.put(name, transform);
    }

    protected Map<String, String> toMap(String[] columnNames, String[] columnValues) {
        Map<String, String> map = new HashMap<String, String>(columnNames.length);
        for (int i = 0; i < columnNames.length; i++) {
            map.put(columnNames[i], columnValues[i]);
        }
        return map;
    }

    @Override
    public boolean isHandlingMissingTable(DataLoaderContext context) {
        List<TransformTable> transformationsToPerform = findTablesToTransform(context
                .getTableTemplate().getFullyQualifiedTableName(), context.getTargetNodeId());
        return transformationsToPerform != null && transformationsToPerform.size() > 0;
    }

}
