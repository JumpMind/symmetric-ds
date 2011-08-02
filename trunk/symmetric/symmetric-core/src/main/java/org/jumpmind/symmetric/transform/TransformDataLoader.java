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

    public boolean filterInsert(IDataLoaderContext context, String[] columnValues) {
        return !transform(DmlType.INSERT, context, context.getCatalogName(), context.getSchemaName(), context.getTableName(), context.getColumnNames(), columnValues, null,
                null, null);
    }

    public boolean filterUpdate(IDataLoaderContext context, String[] columnValues,
            String[] keyValues) {
        return !transform(DmlType.UPDATE, context, context.getCatalogName(), context.getSchemaName(), context.getTableName(), context.getColumnNames(), columnValues,
                context.getKeyNames(), keyValues, context.getOldData());
    }

    public boolean filterDelete(IDataLoaderContext context, String[] keyValues) {
        String[] columnNames = context.getKeyNames();
        String[] columnValues = keyValues;
        if (context.getOldData() != null) {
            columnNames = context.getColumnNames();
            columnValues = context.getOldData();
        }
        return !transform(DmlType.DELETE, context, context.getCatalogName(), context.getSchemaName(), context.getTableName(), columnNames, columnValues,
                context.getKeyNames(), keyValues, context.getOldData());
    }


    @Override
    protected void apply(ICacheContext context, TransformedData data) {
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
                if (table.hasAutoIncrementColumn()) {
                    dbDialect.prepareTableForDataLoad(context.getJdbcTemplate(), table);
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
                    dbDialect.cleanupAfterDataLoad(context.getJdbcTemplate(), table);
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

    public boolean isHandlingMissingTable(IDataLoaderContext context) {
        List<TransformTable> transformationsToPerform = findTablesToTransform(context
                .getTableTemplate().getFullyQualifiedTableName(true),
                parameterService.getNodeGroupId());
        return transformationsToPerform != null && transformationsToPerform.size() > 0;
    }

}
