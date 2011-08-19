package org.jumpmind.symmetric.transform;

import java.util.List;

import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.ext.IBuiltInExtensionPoint;
import org.jumpmind.symmetric.ext.ICacheContext;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IMissingTableHandler;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.load.TableTemplate;
import org.springframework.dao.DataIntegrityViolationException;

public class TransformDataLoader extends AbstractTransformer implements IBuiltInExtensionPoint,
        IDataLoaderFilter, IMissingTableHandler {

    public TransformDataLoader() {
    }

    @Override
    protected TransformPoint getTransformPoint() {
        return TransformPoint.LOAD;
    }

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        boolean processRow = true;
        if (isEligibleForTransform(context.getCatalogName(), context.getSchemaName(),
                context.getTableName())) {
            try {
                List<TransformedData> transformedData = transform(DmlType.INSERT, context,
                        context.getNodeGroupLink(), context.getCatalogName(),
                        context.getSchemaName(), context.getTableName(), context.getColumnNames(),
                        columnValues, null, null, null);
                if (transformedData != null) {
                    apply(context, transformedData);
                    processRow = false;
                }
            } catch (IgnoreRowException ex) {
                processRow = false;
            }
        }
        return processRow;
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues,
            String[] keyValues) {
        boolean processRow = true;
        if (isEligibleForTransform(context.getCatalogName(), context.getSchemaName(),
                context.getTableName())) {
            try {
                List<TransformedData> transformedData = transform(DmlType.UPDATE, context,
                        context.getNodeGroupLink(), context.getCatalogName(),
                        context.getSchemaName(), context.getTableName(), context.getColumnNames(),
                        columnValues, context.getKeyNames(), keyValues, context.getOldData());
                if (transformedData != null) {
                    apply(context, transformedData);
                    processRow = false;
                }
            } catch (IgnoreRowException ex) {
                processRow = false;
            }
        }
        return processRow;
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        String[] columnNames = context.getKeyNames();
        String[] columnValues = keyValues;
        if (context.getOldData() != null) {
            columnNames = context.getColumnNames();
            columnValues = context.getOldData();
        }
        boolean processRow = true;
        if (isEligibleForTransform(context.getCatalogName(), context.getSchemaName(),
                context.getTableName())) {
            try {
                List<TransformedData> transformedData = transform(DmlType.DELETE, context,
                        context.getNodeGroupLink(), context.getCatalogName(),
                        context.getSchemaName(), context.getTableName(), columnNames, columnValues,
                        context.getKeyNames(), keyValues, context.getOldData());
                if (transformedData != null) {
                    apply(context, transformedData);
                    processRow = false;
                }
            } catch (IgnoreRowException ex) {
                processRow = false;
            }
        }
        return processRow;
    }

    protected void apply(ICacheContext context, List<TransformedData> dataThatHasBeenTransformed) {
        for (TransformedData data : dataThatHasBeenTransformed) {
            TableTemplate tableTemplate = new TableTemplate(context.getJdbcTemplate(), dbDialect,
                    data.getTableName(), null, false, data.getSchemaName(), data.getCatalogName());
            tableTemplate.setColumnNames(data.getColumnNames());
            tableTemplate.setKeyNames(data.getKeyNames());
            // TODO Need more advanced fallback logic? Support typical
            // symmetric fallback/recovery settings?
            switch (data.getTargetDmlType()) {
            case INSERT:
                Table table = tableTemplate.getTable();
                try {
                    if (tableTemplate.getFullyQualifiedTableName().equals(
                            context.getContextCache().get(VariableColumnTransform.OPTION_IDENTITY))) {
                        dbDialect.revertAllowIdentityInserts(context.getJdbcTemplate(), table);
                    } else if (table.hasAutoIncrementColumn()) {
                        dbDialect.allowIdentityInserts(context.getJdbcTemplate(), table);
                    }
                    tableTemplate.insert((IDataLoaderContext) context, data.getColumnValues());
                } catch (DataIntegrityViolationException ex) {
                    data.setTargetDmlType(DmlType.UPDATE);
                    tableTemplate.setColumnNames(data.getColumnNames());
                    tableTemplate.setKeyNames(data.getKeyNames());
                    tableTemplate.update((IDataLoaderContext) context, data.getColumnValues(),
                            data.getKeyValues());
                } finally {
                    if (table.hasAutoIncrementColumn()) {
                        dbDialect.revertAllowIdentityInserts(context.getJdbcTemplate(), table);
                    }
                }
                break;
            case UPDATE:
                if (0 == tableTemplate.update((IDataLoaderContext) context, data.getColumnValues(),
                        data.getKeyValues()) && (data.getSourceDmlType() != DmlType.DELETE)) {
                    data.setTargetDmlType(DmlType.INSERT);
                    tableTemplate.setColumnNames(data.getColumnNames());
                    tableTemplate.setKeyNames(data.getKeyNames());
                    tableTemplate.insert((IDataLoaderContext) context, data.getColumnValues());
                }
                break;
            case DELETE:
                tableTemplate.delete((IDataLoaderContext) context, data.getKeyValues());
                break;
            }
        }
    }

    public boolean isHandlingMissingTable(IDataLoaderContext context) {
        List<TransformTable> transformationsToPerform = findTablesToTransform(
                context.getNodeGroupLink(),
                context.getTableTemplate().getFullyQualifiedTableName(true));
        return transformationsToPerform != null && transformationsToPerform.size() > 0;
    }

}
