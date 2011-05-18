package org.jumpmind.symmetric.core.process.sql;

import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.db.IDbPlatform;
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

public class SqlDataWriter implements IDataWriter<SqlDataContext> {

    final Log log = LogFactory.getLog(getClass());

    protected DataSource dataSource;

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

    public SqlDataWriter(DataSource dataSource, IDbPlatform platform, Parameters parameters,
            List<IColumnFilter<DataContext>> columnFilters,
            List<IDataFilter<DataContext>> dataFilters) {

        this.dataSource = dataSource;
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
    }

    public SqlDataContext createDataContext() {
        return new SqlDataContext();
    }

    public void open(SqlDataContext context) {
        this.transaction = this.platform.getSqlConnection().startSqlTransaction();
        this.transaction.setUseBatching(this.useBatching);
    }

    public boolean switchTables(SqlDataContext context) {
        this.lastDataEvent = null;
        Table sourceTable = context.getTable();
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

    public void startBatch(SqlDataContext context) {
        this.lastDataEvent = null;
    }

    public void writeData(Data data, SqlDataContext ctx) {

        try {
            if (data.getEventType() == DataEventType.INSERT) {
                StatementBuilder st = getStatementBuilder(DmlType.INSERT, ctx, data);
                if (this.lastDataEvent != DataEventType.INSERT) {
                    transaction.prepare(st.getSql(), this.maxRowsBeforeBatchFlush, this.useBatching);
                }
                execute(ctx, st, data);
            }

            // TODO UPDATE
            // TODO DELETE

            uncommittedRows++;

            // check if an early commit needs to happen
            if (uncommittedRows > this.maxRowsBeforeCommit) {
                commit();
            }

        } catch (DataIntegrityViolationException ex) {
            if (transaction.isUseBatching() && isCorrectForIntegrityViolation(data)) {
                // if we were in batch mode, then resubmit in non batch mode so
                // we can fallback.
                try {
                    transaction.setUseBatching(false);
                    List<Data> failed = transaction.getUnflushedMarkers();
                    // decrement stats?
                    for (Data data2 : failed) {
                        writeData(data2, ctx);
                    }
                } finally {
                    transaction.setUseBatching(true);
                }
            } else if (isCorrectForIntegrityViolation(data)) {
                // fallback if enabled
            }
        } catch (RuntimeException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
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

    protected int execute(SqlDataContext ctx, StatementBuilder st, Data data) {
        Object[] objectValues = platform.getObjectValues(ctx.getBinaryEncoding(),
                data.toParsedRowData(), st.getMetaData(true));
        if (columnFilters != null) {
            for (IColumnFilter<DataContext> columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, targetTable, objectValues);
            }
        }
        return transaction.update(data, objectValues, st.getTypes());
    }

    final private StatementBuilder getStatementBuilder(DmlType dmlType, SqlDataContext ctx,
            Data data) {
        Column[] statementColumns = targetTable.getColumns();
        StatementBuilder st = ctx.getStatementBuilder(targetTable, data);
        if (st == null) {
            Column[] preFilteredColumns = statementColumns;
            if (columnFilters != null) {
                for (IColumnFilter<DataContext> columnFilter : columnFilters) {
                    statementColumns = columnFilter.filterColumnsNames(ctx, targetTable,
                            statementColumns);
                }
            }

            String tableName = targetTable.getFullyQualifiedTableName();

            st = new StatementBuilder(dmlType, tableName, targetTable.getPrimaryKeyColumnsArray(),
                    targetTable.getColumns(), preFilteredColumns, platform.getPlatformInfo()
                            .isDateOverridesToTimestamp(), platform.getPlatformInfo()
                            .getIdentifierQuoteString());

            if (dmlType != DmlType.UPDATE) {
                ctx.putStatementBuilder(targetTable, data, st);
            }
        }
        return st;
    }

    private void commit() {
        this.transaction.commit();
        uncommittedRows = 0;
    }

    public void close(SqlDataContext context) {
        commit();
        this.transaction.close();
    }

    public void finishBatch(SqlDataContext context) {
        commit();
    }

}
