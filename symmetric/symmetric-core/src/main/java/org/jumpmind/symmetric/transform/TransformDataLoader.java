package org.jumpmind.symmetric.transform;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ext.DataLoaderFilterAdapter;
import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.model.IncomingBatch;
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
            TransformCache cache = getTransformCache(context);
            Map<String, String> originalValues = toMap(columnNames, columnValues);
            Map<String, String> originalkeyValues = originalValues;
            if (keyNames != null) {
                originalkeyValues = toMap(keyNames, keyValues);
            }
            for (TransformTable transformation : transformationsToPerform) {
                TransformedData pkData = getPrimaryKeyValues(transformation, originalkeyValues);
                TransformedData targetData = cache.lookupData(pkData);
                if (targetData == null) {
                    targetData = pkData;
                }

                if (targetData.getDmlType() == null) {
                    targetData.setDmlType(dmlType);
                }

                if (transformData(dmlType, targetData, transformation, originalValues)) {
                    cache.cacheData(targetData);
                }

            }
            return true;
        } else {
            return false;
        }
    }

    protected boolean transformData(DmlType dmlType, TransformedData data,
            TransformTable transformation, Map<String, String> originalValues) {
        boolean persistData = false;
        if (dmlType != DmlType.DELETE) {
            if (dmlType == DmlType.INSERT) {
                data.setDmlType(dmlType);
            }
            for (String columnName : originalValues.keySet()) {
                TransformColumn transformColumn = transformation.getTransformColumnFor(columnName);
                if (transformColumn != null) {
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
                data.setDmlType(DmlType.DELETE);
                persistData = true;
                break;
            case NULL_COL:
                if (dmlType == DmlType.DELETE) {
                    data.setDmlType(DmlType.UPDATE);
                }

                for (TransformColumn transformColumn : transformation.getTransformColumns()) {
                    if (transformColumn != null) {
                        data.put(transformColumn.getTargetColumnName(), null, false);
                    }
                }
                persistData = true;
                break;
            }
        }
        return persistData;
    }

    protected TransformedData getPrimaryKeyValues(TransformTable table,
            Map<String, String> originalValues) {
        TransformedData row = new TransformedData(table.getTargetCatalogName(),
                table.getTargetSchemaName(), table.getTargetTableName());
        List<TransformColumn> columns = table.getPrimaryKeyColumns();
        for (TransformColumn transformColumn : columns) {
            transformColumn(row, transformColumn, originalValues, true);
        }
        return row;
    }

    protected void transformColumn(TransformedData data, TransformColumn transformColumn,
            Map<String, String> originalValues, boolean pk) {
        String value = originalValues.get(transformColumn.getSourceColumnName());
        ITransform transform = transforms.get(transformColumn.getTransformType());
        if (transform != null) {
            value = transform.transform(transformColumn, value);
        }
        data.put(transformColumn.getTargetColumnName(), value, pk);
    }

    @Override
    public void earlyCommit(IDataLoader loader, IncomingBatch batch) {
        batchComplete(loader, batch);
    }

    public void batchComplete(IDataLoader loader, IncomingBatch batch) {
        IDataLoaderContext context = loader.getContext();
        TransformCache cache = getTransformCache(loader.getContext());
        if (cache != null) {
            Iterator<TransformedData> it = cache.dataRows.iterator();
            while (it.hasNext()) {
                TransformedData data = (TransformedData) it.next();
                TableTemplate tableTemplate = new TableTemplate(context.getJdbcTemplate(),
                        dbDialect, data.getTableName(), null, false, data.getSchemaName(),
                        data.getCatalogName());
                tableTemplate.setColumnNames(data.getColumnNames());
                tableTemplate.setKeyNames(data.getKeyNames());
                // TODO Need more advanced fallback logic? Support typical
                // symmetric fallback/recovery settings?
                switch (data.getDmlType()) {
                case INSERT:
                    try {
                        tableTemplate.insert(context, data.getColumnValues());
                    } catch (DataIntegrityViolationException ex) {
                        tableTemplate.update(context, data.getColumnValues(), data.getKeyValues());
                    }
                    break;
                case UPDATE:
                    if (0 == tableTemplate.update(context, data.getColumnValues(),
                            data.getKeyValues())) {
                        tableTemplate.insert(context, data.getColumnValues());
                    }
                    break;
                case DELETE:
                    tableTemplate.delete(context, data.getKeyValues());
                    break;
                }
            }

            cache.clear();
        }
    }

    protected List<TransformTable> findTablesToTransform(String fullyQualifiedSourceTableName,
            String targetNodeId) {
        Map<String, List<TransformTable>> transformMap = transformService.findTransformsFor(
                targetNodeId, true);
        List<TransformTable> transforms = transformMap.get(fullyQualifiedSourceTableName);
        return transforms;
    }

    protected TransformCache getTransformCache(IDataLoaderContext context) {
        TransformCache cache = (TransformCache) context.getContextCache().get(CACHE_KEY);
        if (cache == null) {
            cache = new TransformCache();
            context.getContextCache().put(CACHE_KEY, cache);
        }
        return cache;
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

    class TransformCache {

        protected List<TransformedData> dataRows = new ArrayList<TransformedData>();
        protected Map<String, Map<String, TransformedData>> indexedDataRows = new HashMap<String, Map<String, TransformedData>>();

        protected TransformedData lookupData(TransformedData pk) {
            TransformedData row = null;
            Map<String, TransformedData> rows = indexedDataRows
                    .get(pk.getFullyQualifiedTableName());
            if (rows != null) {
                row = rows.get(pk.getKeyString());
            }

            return row;
        }

        protected void cacheData(TransformedData data) {
            if (lookupData(data) == null) {
                dataRows.add(data);
                Map<String, TransformedData> rows = indexedDataRows.get(data
                        .getFullyQualifiedTableName());
                if (rows == null) {
                    rows = new HashMap<String, TransformedData>();
                    indexedDataRows.put(data.getFullyQualifiedTableName(), rows);
                }
                rows.put(data.getKeyString(), data);
            }
        }

        protected void clear() {
            dataRows.clear();
            indexedDataRows.clear();
        }

    }
}
