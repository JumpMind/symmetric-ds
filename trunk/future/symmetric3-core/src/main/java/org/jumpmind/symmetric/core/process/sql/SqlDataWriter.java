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
import org.jumpmind.symmetric.core.sql.StatementBuilder;
import org.jumpmind.symmetric.core.sql.StatementBuilder.DmlType;

public class SqlDataWriter implements IDataWriter<SqlDataContext> {

    final Log log = LogFactory.getLog(getClass());

    protected DataSource dataSource;

    protected IDbPlatform platform;

    protected Parameters parameters;

    protected List<IColumnFilter<DataContext>> columnFilters;

    protected List<IDataFilter<DataContext>> dataFilters;

    public SqlDataWriter(DataSource dataSource, IDbPlatform platform, Parameters parameters,
            List<IColumnFilter<DataContext>> columnFilters,
            List<IDataFilter<DataContext>> dataFilters) {
        this.dataSource = dataSource;
        this.platform = platform;
        this.parameters = parameters != null ? parameters : new Parameters();
        this.columnFilters = columnFilters;
        this.dataFilters = dataFilters;
    }

    public SqlDataContext createDataContext() {
        return new SqlDataContext();
    }

    public void open(SqlDataContext context) {
//        try {
//            context.setConnection(dataSource.getConnection());
//            context.setOldAutoCommitValue(context.getConnection().getAutoCommit());
//            context.getConnection().setAutoCommit(false);
//        } catch (SQLException ex) {
//            throw new DbException(ex);
//        }
    }

    public boolean switchTables(SqlDataContext context) {
        Table sourceTable = context.getSourceTable();
        if (sourceTable != null) {
            Table targetTable = platform.findTable(sourceTable.getCatalogName(),
                    sourceTable.getSchemaName(), sourceTable.getTableName(), true, parameters)
                    .copy();
            if (targetTable != null) {
                targetTable.reOrderColumns(sourceTable.getColumns(),
                        parameters.is(Parameters.DB_USE_PKS_FROM_SOURCE, true));
                context.setTargetTable(targetTable);
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
    }

    public void writeData(Data data, SqlDataContext ctx) {
        if (data.getEventType() == DataEventType.INSERT) {
            StatementBuilder st = getStatementBuilder(DmlType.INSERT, ctx, data);
            execute(ctx, st, data);
        }

        // check if an early commit needs to happen
    }

    protected int execute(SqlDataContext ctx, StatementBuilder st, Data data) {
        Object[] objectValues = platform.getObjectValues(ctx.getBinaryEncoding(),
                data.toParsedRowData(), st.getMetaData(true));
        if (columnFilters != null) {
            for (IColumnFilter<DataContext> columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, ctx.getTargetTable(),
                        objectValues);
            }
        }
        return platform.getSqlConnection().update(st.getSql(), objectValues, st.getTypes());
    }

    final private StatementBuilder getStatementBuilder(DmlType dmlType, SqlDataContext ctx,
            Data data) {
        Table targetTable = ctx.getTargetTable();
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

            String tableName = ctx.getTargetTable().getFullyQualifiedTableName();

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

    public void close(SqlDataContext context) {
        //context.close();
    }

    public void finishBatch(SqlDataContext context) {
        //context.commit();
    }

}
