package org.jumpmind.symmetric.core.process.sql;

import java.util.ArrayList;
import java.util.List;

import org.jumpmind.symmetric.core.common.ArrayUtils;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.db.IDbPlatform;
import org.jumpmind.symmetric.core.model.Batch;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IColumnFilter;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.IDataWriter;
import org.jumpmind.symmetric.core.sql.DataIntegrityViolationException;
import org.jumpmind.symmetric.core.sql.ISqlTransaction;
import org.jumpmind.symmetric.core.sql.StatementBuilder;
import org.jumpmind.symmetric.core.sql.StatementBuilder.DmlType;

// Notes:  Inserts try to use jdbc batching by default
public class SqlDataWriter implements IDataWriter<DataContext> {

    final Log log = LogFactory.getLog(getClass());

    protected IDbPlatform platform;

    protected Parameters parameters;

    protected List<IColumnFilter<DataContext>> columnFilters;

    protected List<IDataFilter<DataContext>> dataFilters;

    protected Table targetTable;

    protected ISqlTransaction transaction;

    protected DataEventType lastDataEvent;

    protected int uncommittedRows = 0;

    protected int maxRowsBeforeBatchFlush;

    protected boolean enableFallbackForInsert;

    protected boolean enableFallbackForUpdate;

    protected boolean allowMissingDeletes;

    protected boolean useBatching;

    protected int maxRowsBeforeCommit;

    protected boolean usePrimaryKeysFromSource;

    protected boolean dontIncludeKeysInUpdateStatement;

    protected Batch batch;

    protected DataContext ctx;

    public SqlDataWriter(IDbPlatform platform, Parameters parameters) {
        this(platform, parameters, null, null);
    }

    public SqlDataWriter(IDbPlatform platform, Parameters parameters,
            List<IColumnFilter<DataContext>> columnFilters,
            List<IDataFilter<DataContext>> dataFilters) {
        this.platform = platform;
        this.parameters = parameters != null ? parameters : new Parameters();
        this.columnFilters = columnFilters;
        this.dataFilters = dataFilters;

        this.maxRowsBeforeBatchFlush = parameters.getInt(
                Parameters.LOADER_MAX_ROWS_BEFORE_BATCH_FLUSH, 10);
        this.enableFallbackForInsert = parameters
                .is(Parameters.LOADER_ENABLE_FALLBACK_INSERT, true);
        this.enableFallbackForUpdate = parameters
                .is(Parameters.LOADER_ENABLE_FALLBACK_UPDATE, true);
        this.allowMissingDeletes = parameters.is(Parameters.LOADER_ALLOW_MISSING_DELETES, true);
        this.useBatching = parameters.is(Parameters.LOADER_USE_BATCHING, true);
        this.maxRowsBeforeCommit = parameters
                .getInt(Parameters.LOADER_MAX_ROWS_BEFORE_COMMIT, 1000);
        this.usePrimaryKeysFromSource = parameters.is(Parameters.DB_USE_PKS_FROM_SOURCE, true);
        this.dontIncludeKeysInUpdateStatement = parameters.is(
                Parameters.LOADER_DONT_INCLUDE_PKS_IN_UPDATE, false);
    }

    public DataContext createDataContext() {
        return new DataContext();
    }

    public void open(DataContext context) {
        this.ctx = context;
        this.transaction = this.platform.getSqlConnection().startSqlTransaction();
    }

    public boolean switchTables(Table sourceTable) {
        this.lastDataEvent = null;
        if (sourceTable != null) {
            this.targetTable = platform.findTable(sourceTable.getCatalogName(),
                    sourceTable.getSchemaName(), sourceTable.getTableName(), true).copy();
            if (this.targetTable != null) {
                this.targetTable.reOrderColumns(sourceTable.getColumns(),
                        this.usePrimaryKeysFromSource);
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
        this.lastDataEvent = null;
        this.batch = batch;
    }

    public void writeData(Data data) {
        writeData(data, this.useBatching);
        lastDataEvent = data.getEventType();
    }

    protected void writeData(Data data, boolean batchMode) {

        try {
            switch (data.getEventType()) {
            case INSERT:
                processInsert(data, batchMode);
                break;

            case UPDATE:
                processUpdate(data);
                break;

            case DELETE:
                processDelete(data, batchMode);
                break;

            case SQL:
                processSql(data);
                break;
            }

            uncommittedRows++;

            // check if an early commit needs to happen
            if (uncommittedRows > this.maxRowsBeforeCommit) {
                commit();
            }

        } catch (DataIntegrityViolationException ex) {
            if (transaction.isInBatchMode() && isCorrectForIntegrityViolation(data)) {
                // if we were in batch mode, then resubmit in non-batch mode so
                // we can fallback.
                List<Data> failed = transaction.getUnflushedMarkers();
                batch.decrementInsertCount(failed.size());
                for (Data data2 : failed) {
                    writeData(data2, false);
                }
            } else {
                throw ex;
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    protected boolean filterData(Data data, DataContext ctx) {
        boolean continueToLoad = true;
        if (dataFilters != null) {
            batch.startTimer();
            for (IDataFilter<DataContext> filter : dataFilters) {
                continueToLoad &= filter.filter(ctx, data);
            }
            batch.incrementFilterMillis(batch.endTimer());
        }
        return continueToLoad;
    }

    protected boolean doesColumnNeedUpdated(int columnIndex, Column column, Data data) {
        boolean needsUpdated = true;
        String[] oldData = data.toParsedOldData();
        String[] rowData = data.toParsedRowData();
        if (oldData != null) {
            needsUpdated = !StringUtils.equals(rowData[columnIndex], oldData[columnIndex])
                    || (platform.isLob(column.getTypeCode()) && (platform.getPlatformInfo()
                            .isNeedsToSelectLobData() || StringUtils.isBlank(oldData[columnIndex])));
        } else if (dontIncludeKeysInUpdateStatement) {
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
        StatementBuilder st = null;
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
            st = getStatementBuilder(DmlType.UPDATE, targetTable.getPrimaryKeyColumnsArray(),
                    changedColumnMetaList.toArray(new Column[changedColumnMetaList.size()]));
            columnValues = (String[]) changedColumnValueList
                    .toArray(new String[changedColumnValueList.size()]);
            String[] values = (String[]) ArrayUtils.addAll(columnValues, getPkData(data));
            transaction.prepare(st.getSql(), -1, false);
            return execute(st, data, values);
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
        StatementBuilder st = getStatementBuilder(DmlType.INSERT, null, targetTable.getColumns());
        if (this.lastDataEvent != DataEventType.INSERT) {
            transaction.prepare(st.getSql(), this.maxRowsBeforeBatchFlush, batchMode);
        }
        execute(st, data, data.toParsedRowData());
    }

    protected void processInsert(Data data, boolean batchMode) {
        if (filterData(data, ctx)) {
            batch.incrementInsertCount();
            try {
                batch.startTimer();
                // TODO add save point logic for postgresql
                executeInsertSql(data, batchMode);
            } catch (DataIntegrityViolationException e) {
                // TODO log insert failed
                if (enableFallbackForInsert && !batchMode) {
                    this.lastDataEvent = null;
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

    protected void processUpdate(Data data) {
        if (filterData(data, ctx)) {
            batch.incrementUpdateCount();
            try {
                batch.startTimer();
                executeUpdateSql(data);
            } catch (DataIntegrityViolationException e) {
                // TODO log update failed
                if (enableFallbackForUpdate) {
                    batch.incrementFallbackInsertCount();
                    executeInsertSql(data, false);
                } else {
                    throw e;
                }
            } finally {
                batch.incrementDatabaseMillis(batch.endTimer());
            }
        }
    }

    protected void processDelete(Data data, boolean batchMode) {
        // TODO
    }

    protected void processSql(Data data) {
        // TODO
    }

    protected boolean isCorrectForIntegrityViolation(Data data) {
        if (data.getEventType() == DataEventType.INSERT && enableFallbackForInsert) {
            return true;
        } else if (data.getEventType() == DataEventType.UPDATE && enableFallbackForUpdate) {
            return true;
        } else if (data.getEventType() == DataEventType.DELETE && allowMissingDeletes) {
            return true;
        } else {
            return false;
        }
    }

    protected int execute(StatementBuilder st, Data data, String[] values) {
        Object[] objectValues = platform.getObjectValues(ctx.getBinaryEncoding(), values,
                st.getMetaData(true));
        if (columnFilters != null) {
            for (IColumnFilter<DataContext> columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, targetTable, objectValues);
            }
        }
        return transaction.update(data, objectValues, st.getTypes());
    }

    final private StatementBuilder getStatementBuilder(DmlType dmlType, Column[] lookupColumns,
            Column[] changingColumns) {
        Column[] preFilteredColumns = changingColumns;
        if (columnFilters != null) {
            for (IColumnFilter<DataContext> columnFilter : columnFilters) {
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

}
