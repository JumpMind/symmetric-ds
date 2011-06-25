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
        List<TransformTable> tablesToTransform = findTablesToTransform(context);
        if (tablesToTransform != null && tablesToTransform.size() > 0) {
            TransformCache cache = getTransformCache(context);
            Map<String, String> originalValues = toMap(columnNames, columnValues);
            Map<String, String> originalkeyValues = originalValues;
            if (keyNames != null) {
                originalkeyValues = toMap(keyNames, keyValues);
            }
            for (TransformTable transformTable : tablesToTransform) {
                TransformedData pk = getPrimaryKeyValues(transformTable,
                        originalkeyValues);
                TransformedData row = cache.lookupRow(pk);
                if (row == null) {
                    row = pk;
                }

                if (row.getDmlType() == null) {
                    row.setDmlType(dmlType);
                }

                if (dmlType != DmlType.DELETE) {
                    if (dmlType == DmlType.INSERT) {
                        row.setDmlType(dmlType);
                    }
                    for (String columnName : columnNames) {
                        TransformColumn transformColumn = transformTable
                                .getTransformColumnFor(columnName);
                        if (transformColumn != null) {
                            transform(row, transformColumn, originalValues, false);
                        }
                    }

                    cache.cacheRow(row);
                } else {
                    // handle the delete action
                    DeleteAction deleteAction = transformTable.getDeleteAction();
                    switch (deleteAction) {
                    case NONE:
                        break;
                    case DEL_ROW:
                        row.setDmlType(DmlType.DELETE);
                        cache.cacheRow(row);
                        break;
                    case NULL_COL:
                        if (dmlType == DmlType.DELETE) {
                            row.setDmlType(DmlType.UPDATE);
                        }

                        for (String columnName : columnNames) {
                            TransformColumn transformColumn = transformTable
                                    .getTransformColumnFor(columnName);
                            if (transformColumn != null) {
                                row.put(transformColumn.getTargetColumnName(), null, false);
                            }
                        }
                        cache.cacheRow(row);
                        break;
                    }
                }
            }
            return true;
        } else {
            return false;
        }
    }

    protected TransformedData getPrimaryKeyValues(TransformTable table,
            Map<String, String> originalValues) {
        TransformedData row = new TransformedData(table.getTargetCatalogName(),
                table.getTargetSchemaName(), table.getTargetTableName());
        List<TransformColumn> columns = table.getPrimaryKeyColumns();
        for (TransformColumn transformColumn : columns) {
            transform(row, transformColumn, originalValues, true);
        }
        return row;
    }

    protected void transform(TransformedData row, TransformColumn transformColumn,
            Map<String, String> originalValues, boolean pk) {
        String value = originalValues.get(transformColumn.getSourceColumnName());
        ITransform transform = transforms.get(transformColumn.getTransformType());
        if (transform != null) {
            value = transform.transform(transformColumn, value);
        }
        row.put(transformColumn.getTargetColumnName(), value, pk);
    }

    public void batchComplete(IDataLoader loader, IncomingBatch batch) {
        IDataLoaderContext context = loader.getContext();
        TransformCache cache = getTransformCache(loader.getContext());
        if (cache != null) {
            Iterator<TransformedData> it = cache.dataRows.iterator();
            while (it.hasNext()) {
                TransformedData row = (TransformedData) it.next();
                TableTemplate tableTemplate = new TableTemplate(context.getJdbcTemplate(),
                        dbDialect, row.getTableName(), null, false, row.getSchemaName(),
                        row.getCatalogName());
                tableTemplate.setColumnNames(row.getColumnNames());
                tableTemplate.setKeyNames(row.getKeyNames());
                // TODO Need more advanced fallback logic? Support typical
                // symmetric fallback/recovery settings?
                switch (row.getDmlType()) {
                case INSERT:
                    try {
                        tableTemplate.insert(context, row.getColumnValues());
                    } catch (DataIntegrityViolationException ex) {
                        tableTemplate.update(context, row.getColumnValues(), row.getKeyValues());
                    }
                    break;
                case UPDATE:
                    if (0 == tableTemplate.update(context, row.getColumnValues(),
                            row.getKeyValues())) {
                        tableTemplate.insert(context, row.getColumnValues());
                    }
                    break;
                case DELETE:
                    tableTemplate.delete(context, row.getKeyValues());
                    break;
                }
            }

            cache.clear();
        }
    }

    protected List<TransformTable> findTablesToTransform(IDataLoaderContext context) {
        Map<String, List<TransformTable>> transformMap = transformService.findTransformsFor(context
                .getTargetNode().getNodeGroupId(), true);
        List<TransformTable> transforms = transformMap.get(context.getTableTemplate()
                .getFullyQualifiedTableName());
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
        List<TransformTable> tablesToTransform = findTablesToTransform(context);
        return tablesToTransform != null && tablesToTransform.size() > 0;
    }

    class TransformCache {

        protected List<TransformedData> dataRows = new ArrayList<TransformedData>();
        protected Map<String, Map<String, TransformedData>> indexedDataRows = new HashMap<String, Map<String, TransformedData>>();

        protected TransformedData lookupRow(TransformedData pk) {
            TransformedData row = null;
            Map<String, TransformedData> rows = indexedDataRows
                    .get(pk.getFullyQualifiedTableName());
            if (rows != null) {
                row = rows.get(pk.getKeyString());
            }

            return row;
        }        

        protected void cacheRow(TransformedData row) {
            if (lookupRow(row) == null) {
                dataRows.add(row);
                Map<String, TransformedData> rows = indexedDataRows.get(row
                        .getFullyQualifiedTableName());
                if (rows == null) {
                    rows = new HashMap<String, TransformedData>();
                    indexedDataRows.put(row.getFullyQualifiedTableName(), rows);
                }
                rows.put(row.getKeyString(), row);
            }
        }

        protected void clear() {
            dataRows.clear();
            indexedDataRows.clear();
        }

    }
}
