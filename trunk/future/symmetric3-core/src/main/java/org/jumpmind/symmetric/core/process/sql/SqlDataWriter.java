package org.jumpmind.symmetric.core.process.sql;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.common.ArrayUtils;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.AbstractDataFilter;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.DataFailedToLoadException;
import org.jumpmind.symmetric.core.process.DataProcessor;
import org.jumpmind.symmetric.core.process.IColumnFilter;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.IDataWriter;
import org.jumpmind.symmetric.core.sql.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.sql.ISqlTransaction;
import org.jumpmind.symmetric.core.sql.SqlScript;
import org.jumpmind.symmetric.core.sql.StatementBuilder;
import org.jumpmind.symmetric.core.sql.StatementBuilder.DmlType;

/**
 * An {@link IDataWriter} used by {@link DataProcessor}s to write {@link Data}
 * to a relational database.
 */
public class SqlDataWriter extends AbstractDataFilter implements IDataWriter {

    protected IDbPlatform platform;

    protected Table targetTable;

    protected ISqlTransaction transaction;

    protected StatementBuilder statementBuilder;

    protected Data lastData;

    protected int uncommittedRows = 0;

    protected Settings settings;

    protected Batch batch;

    protected DataContext ctx;

    protected boolean currentBatchMode = false;

    public SqlDataWriter(IDbPlatform platform) {
        this(platform, new Settings());
    }

    public SqlDataWriter(IDbPlatform platform, Parameters parameters) {
        this(platform, parameters, null, null);
    }

    public SqlDataWriter(IDbPlatform platform, Parameters parameters,
            IDataFilter filter) {
        this(platform, parameters, null, toList(filter));
    }

    public SqlDataWriter(IDbPlatform platform, Settings settings) {
        this(platform, settings, null, null);
    }

    public SqlDataWriter(IDbPlatform platform, Parameters parameters,
            List<IColumnFilter> columnFilters,
            List<IDataFilter> dataFilters) {
        this(platform, new Settings(), columnFilters, dataFilters);
        populateSettings(parameters);
    }

    public SqlDataWriter(IDbPlatform platform, Settings settings,
            List<IColumnFilter> columnFilters,
            List<IDataFilter> dataFilters) {
        this.platform = platform;
        this.columnFilters = columnFilters;
        this.dataFilters = dataFilters;
        this.settings = settings == null ? new Settings() : settings;
    }

    public void open(DataContext context) {
        this.ctx = context;
        this.transaction = this.platform.getSqlConnection().startSqlTransaction();
        this.transaction.setNumberOfRowsBeforeBatchFlush(settings.maxRowsBeforeBatchFlush);
        this.ctx.getContext().put(DataContext.KEY_SQL_TRANSACTION, this.transaction);
    }

    public boolean writeTable(Table sourceTable) {
        if (sourceTable != null) {
            Table tableAtTarget = platform.findTable(sourceTable.getCatalogName(),
                    sourceTable.getSchemaName(), sourceTable.getTableName(), true);
            if (tableAtTarget == null && settings.autoCreateTable) {
                new SqlScript(platform.getAlterScriptFor(sourceTable), platform).execute();
                tableAtTarget = platform.findTable(sourceTable.getCatalogName(),
                        sourceTable.getSchemaName(), sourceTable.getTableName(), true);
            }

            if (tableAtTarget != null) {
                this.targetTable = tableAtTarget.copy();
                this.targetTable.reOrderColumns(sourceTable.getColumns(),
                        settings.usePrimaryKeysFromSource);
                return true;
            } else {
                log.log(LogLevel.WARN, "Did not find the %s table in the target database",
                        sourceTable.getTableName());
                return false;
            }
        } else {
            throw new IllegalStateException(
                    "switchTables() should not be called without setting the source table in the context");
        }

    }

    public void startBatch(Batch batch) {
        this.statementBuilder = null;
        this.batch = batch;
        this.currentBatchMode = settings.batchMode;
    }

    public boolean writeData(Data data) {        
        boolean committed = writeData(data, currentBatchMode, true);
        this.lastData = data;
        return committed;
    }

    protected boolean writeData(Data data, boolean batchMode, boolean filter) {
        boolean committed = false;
        try {
            if (requireNewStatement(data)) {
                flush();
            }

            switch (data.getEventType()) {
            case INSERT:
                processInsert(data, batchMode, filter);
                break;

            case UPDATE:
                processUpdate(data, filter);
                break;

            case DELETE:
                processDelete(data, batchMode, filter);
                break;

            case SQL:
                processSql(data, filter);
                break;
            }

            uncommittedRows++;

            // check if an early commit needs to happen
            if (uncommittedRows > settings.maxRowsBeforeCommit) {
                commit();
                committed = true;
            }

        } catch (DataIntegrityViolationException ex) {
            handleDataIntegrityViolationException(ex);
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new DataFailedToLoadException(data, ex);
        }
        
        return committed;
        
    }

    protected void handleDataIntegrityViolationException(DataIntegrityViolationException ex) {
        if (transaction.isInBatchMode()) {
            log.log(LogLevel.WARN, "Exiting batch mode for the rest of batch %d",
                    batch.getBatchId());
            this.currentBatchMode = false;
            resendFailedDataInNonBatchMode();
        } else {
            throw ex;
        }
    }

    protected void flush() {
        try {
            transaction.flush();
        } catch (DataIntegrityViolationException ex) {
            handleDataIntegrityViolationException(ex);
        }
    }

    protected boolean requireNewStatement(Data data) {
        return statementBuilder == null || lastData == null
                || lastData.getEventType() != data.getEventType();
    }

    protected void resendFailedDataInNonBatchMode() {
        List<Data> failed = transaction.getUnflushedMarkers(true);
        batch.decrementInsertCount(failed.size());
        for (Data data2 : failed) {
            writeData(data2, false, false);
        }
    }

    protected void populateSettings(Parameters parameters) {
        settings.maxRowsBeforeBatchFlush = parameters.getInt(
                Parameters.LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH, 10);
        settings.enableFallbackForInsert = parameters.is(Parameters.LOADER_ENABLE_FALLBACK_INSERT,
                true);
        settings.enableFallbackForUpdate = parameters.is(Parameters.LOADER_ENABLE_FALLBACK_UPDATE,
                true);
        settings.allowMissingDeletes = parameters.is(Parameters.LOADER_ALLOW_MISSING_DELETES, true);
        settings.batchMode = parameters.is(Parameters.LOADER_USE_BATCHING, true);
        settings.maxRowsBeforeCommit = parameters.getInt(Parameters.LOADER_MAX_ROWS_BEFORE_COMMIT,
                1000);
        settings.usePrimaryKeysFromSource = parameters.is(Parameters.DB_USE_PKS_FROM_SOURCE, true);
        settings.dontIncludeKeysInUpdateStatement = parameters.is(
                Parameters.LOADER_DONT_INCLUDE_PKS_IN_UPDATE, false);
        settings.autoCreateTable = parameters.is(Parameters.LOADER_CREATE_TABLE_IF_DOESNT_EXIST,
                false);

        List<IDataFilter> filters = parameters
                .instantiate(Parameters.LOADER_DATA_FILTERS);
        if (filters.size() > 0) {
            if (this.dataFilters == null) {
                this.dataFilters = filters;
            } else {
                this.dataFilters.addAll(filters);
            }
        }
    }

    protected boolean doesColumnNeedUpdated(int columnIndex, Column column, Data data) {
        boolean needsUpdated = true;
        String[] oldData = data.toParsedOldData();
        String[] rowData = data.toParsedRowData();
        if (oldData != null) {
            needsUpdated = !StringUtils.equals(rowData[columnIndex], oldData[columnIndex])
                    || (platform.isLob(column.getTypeCode()) && (platform.getPlatformInfo()
                            .isNeedsToSelectLobData() || StringUtils.isBlank(oldData[columnIndex])));
        } else if (settings.dontIncludeKeysInUpdateStatement) {
            // This is in support of creating update statements that don't use
            // the keys in the set portion of the update statement. </p> In
            // oracle (and maybe not only in oracle) if there is no index on
            // child table on FK column and update is performing on PK on master
            // table, table lock is acquired on child table. Table lock is taken
            // not in exclusive mode, but lock contentions is possible.
            //
            // @see ParameterConstants#DATA_LOADER_NO_KEYS_IN_UPDATE
            needsUpdated = !column.isPrimaryKey()
                    || !StringUtils.equals(rowData[columnIndex], getPkDataFor(data, column));
        }
        return needsUpdated;
    }

    protected String getPkDataFor(Data data, Column column) {
        String[] values = data.toParsedPkData();
        if (values != null) {
            Column[] columns = targetTable.getColumns();
            int index = -1;
            for (Column column2 : columns) {
                if (column2.isPrimaryKey()) {
                    index++;
                }
                if (column2.equals(column)) {
                    return values[index];
                }
            }
        } else {
            return data.toParsedRowData()[targetTable.getColumnIndex(column)];
        }
        return null;
    }

    protected int executeUpdateSql(Data data) {
        String[] columnValues = data.toParsedRowData();
        ArrayList<String> changedColumnNameList = new ArrayList<String>();
        ArrayList<String> changedColumnValueList = new ArrayList<String>();
        ArrayList<Column> changedColumnMetaList = new ArrayList<Column>();
        for (int i = 0; i < columnValues.length; i++) {
            Column column = targetTable.getColumn(i);
            if (column != null) {
                if (doesColumnNeedUpdated(i, column, data)) {
                    changedColumnNameList.add(column.getName());
                    changedColumnMetaList.add(column);
                    changedColumnValueList.add(columnValues[i]);
                }
            }
        }
        if (changedColumnNameList.size() > 0) {
            this.statementBuilder = getStatementBuilder(DmlType.UPDATE,
                    targetTable.getPrimaryKeyColumnsArray(),
                    changedColumnMetaList.toArray(new Column[changedColumnMetaList.size()]));
            columnValues = (String[]) changedColumnValueList
                    .toArray(new String[changedColumnValueList.size()]);
            String[] values = (String[]) ArrayUtils.addAll(columnValues, getPkData(data));
            transaction.setInBatchMode(false);
            transaction.prepare(this.statementBuilder.getSql());
            return execute(data, values);
        } else {
            // There was no change to apply
            return 1;
        }
    }

    protected String[] getPkData(Data data) {
        String[] pkData = data.toParsedPkData();
        if (pkData == null) {
            String[] values = data.toParsedRowData();
            Column[] pkColumns = targetTable.getPrimaryKeyColumnsArray();
            pkData = new String[pkColumns.length];
            int i = 0;
            for (Column column : pkColumns) {
                pkData[i++] = values[targetTable.getColumnIndex(column)];
            }
        }
        return pkData;
    }

    protected void executeInsertSql(Data data, boolean batchMode) {
        transaction.setInBatchMode(batchMode);
        if (requireNewStatement(data)) {
            this.statementBuilder = getStatementBuilder(DmlType.INSERT, null,
                    targetTable.getColumns());
            transaction.prepare(this.statementBuilder.getSql());
        }
        execute(data, data.toParsedRowData());
    }

    protected int executeDeleteSql(Data data, boolean batchMode) {
        transaction.setInBatchMode(batchMode);
        if (requireNewStatement(data)) {
            this.statementBuilder = getStatementBuilder(DmlType.DELETE,
                    targetTable.getPrimaryKeyColumnsArray(), targetTable.getColumns());
            transaction.prepare(this.statementBuilder.getSql());
        }
        return execute(data, data.toParsedPkData());
    }

    protected void processInsert(Data data, boolean batchMode, boolean filter) {
        if (!filter || filterData(data, batch, targetTable, ctx)) {
            try {
                batch.startTimer();
                // TODO add save point logic for postgresql
                executeInsertSql(data, batchMode);
                batch.incrementInsertCount();
            } catch (DataIntegrityViolationException e) {
                if (settings.enableFallbackForInsert && !batchMode) {
                    this.statementBuilder = null;
                    // TODO rollback to save point
                    batch.incrementFallbackUpdateCount();
                    executeUpdateSql(data);
                } else {
                    throw e;
                }
            } finally {
                batch.incrementDatabaseMillis(batch.endTimer());
            }
        }
    }

    protected void processUpdate(Data data, boolean filter) {
        if (!filter || filterData(data, batch, targetTable, ctx)) {
            try {
                batch.startTimer();
                int updateCount = executeUpdateSql(data);
                if (updateCount == 0) {
                    if (settings.enableFallbackForUpdate) {
                        // The row was missing, fallback to an insert
                        executeInsertSql(data, false);
                        batch.incrementFallbackInsertCount();
                    } else {
                        throw new DataFailedToLoadException(data, "There were no rows to update");
                    }
                } else {
                    batch.incrementUpdateCount();
                }
            } catch (DataIntegrityViolationException e) {
                // If we got here, most likely scenario is that the update
                // has already run and updated the primary key.
                // Let's attempt to run the update using the new
                // key values.
                if (settings.enableFallbackForUpdate) {
                    // remove the old pk values so that the new ones will be
                    // used
                    data.clearPkData();
                    int updateCount = executeUpdateSql(data);
                    if (updateCount == 0) {
                        throw new DataFailedToLoadException(data,
                                "There were no rows to update using");
                    } else {
                        batch.incrementFallbackUpdateWithNewKeysCount();
                    }
                } else {
                    throw e;
                }
            } finally {
                batch.incrementDatabaseMillis(batch.endTimer());
            }
        }
    }

    protected void processDelete(Data data, boolean batchMode, boolean filter) {
        if (!filter || filterData(data, batch, targetTable, ctx)) {
            batch.startTimer();
            try {
                int updateCount = executeDeleteSql(data, batchMode);
                if (!batchMode && updateCount == 0) {
                    batch.incrementMissingDeleteCount();
                    if (!settings.allowMissingDeletes) {
                        throw new DataFailedToLoadException(data, "No rows were deleted");
                    }
                } else {
                    batch.incrementDeleteCount();
                }
            } finally {
                batch.incrementDatabaseMillis(batch.endTimer());
            }
        }
    }

    protected void processSql(Data data, boolean filter) {
        if (!filter || filterData(data, batch, targetTable, ctx)) {
            transaction.setInBatchMode(false);
            String[] tokens = data.toParsedRowData();
            if (tokens != null && tokens.length > 0) {
                transaction.prepare(tokens[0]);
                batch.incrementSqlRowsAffected(transaction.update(data));
                batch.incrementSqlCount();
            }
        }
    }

    protected boolean isCorrectForIntegrityViolation(Data data) {
        if (data.getEventType() == DataEventType.INSERT && settings.enableFallbackForInsert) {
            return true;
        } else if (data.getEventType() == DataEventType.UPDATE && settings.enableFallbackForUpdate) {
            return true;
        } else if (data.getEventType() == DataEventType.DELETE && settings.allowMissingDeletes) {
            return true;
        } else {
            return false;
        }
    }

    protected int execute(Data data, String[] values) {
        Object[] objectValues = platform.getObjectValues(ctx.getBinaryEncoding(), values,
                statementBuilder.getMetaData(true));
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, targetTable, objectValues);
            }
        }
        return transaction.update(data, objectValues, this.statementBuilder.getTypes());
    }

    final private StatementBuilder getStatementBuilder(DmlType dmlType, Column[] lookupColumns,
            Column[] changingColumns) {
        Column[] preFilteredColumns = changingColumns;
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                changingColumns = columnFilter
                        .filterColumnsNames(ctx, targetTable, changingColumns);
            }
        }

        String tableName = targetTable.getFullyQualifiedTableName();

        return new StatementBuilder(dmlType, tableName, lookupColumns, changingColumns,
                preFilteredColumns, platform.getPlatformInfo().isDateOverridesToTimestamp(),
                platform.getPlatformInfo().getIdentifierQuoteString());

    }

    private void commit() {
        flush();
        this.transaction.commit();
        uncommittedRows = 0;
    }

    public void close() {
        commit();
        this.transaction.close();
    }

    public void finishBatch(Batch batch) {
        commit();
    }

    public static class Settings {

        protected int maxRowsBeforeBatchFlush;

        protected boolean enableFallbackForInsert;

        protected boolean enableFallbackForUpdate;

        protected boolean allowMissingDeletes;

        protected boolean batchMode;

        protected int maxRowsBeforeCommit;

        protected boolean usePrimaryKeysFromSource;

        protected boolean dontIncludeKeysInUpdateStatement;

        protected boolean autoCreateTable;

    }

}
