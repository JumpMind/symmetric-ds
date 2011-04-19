package org.jumpmind.symmetric.jdbc.process;

import java.sql.SQLException;
import java.util.List;

import javax.sql.DataSource;

import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.LogLevel;
import org.jumpmind.symmetric.core.db.DbException;
import org.jumpmind.symmetric.core.db.IPlatform;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.Data;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Parameters;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.process.DataContext;
import org.jumpmind.symmetric.core.process.IColumnFilter;
import org.jumpmind.symmetric.core.process.IDataFilter;
import org.jumpmind.symmetric.core.process.IDataWriter;
import org.jumpmind.symmetric.jdbc.sql.StatementBuilder;
import org.jumpmind.symmetric.jdbc.sql.StatementBuilder.DmlType;

public class JdbcDataWriter implements IDataWriter<JdbcDataContext> {

    final Log log = LogFactory.getLog(getClass());

    protected DataSource dataSource;

    protected IPlatform platform;

    protected Parameters parameters;

    protected List<IColumnFilter<DataContext>> columnFilters;

    protected List<IDataFilter<DataContext>> dataFilters;

    public JdbcDataWriter(DataSource dataSource, IPlatform platform, Parameters parameters,
            List<IColumnFilter<DataContext>> columnFilters,
            List<IDataFilter<DataContext>> dataFilters) {
        this.dataSource = dataSource;
        this.platform = platform;
        this.parameters = parameters != null ? parameters : new Parameters();
        this.columnFilters = columnFilters;
        this.dataFilters = dataFilters;
    }

    public JdbcDataContext createDataContext() {
        return new JdbcDataContext();
    }

    public void open(JdbcDataContext context) {
        try {
            context.setConnection(dataSource.getConnection());
            context.setOldAutoCommitValue(context.getConnection().getAutoCommit());
            context.getConnection().setAutoCommit(false);
        } catch (SQLException ex) {
            throw new DbException(ex);
        }
    }

    public boolean switchTables(JdbcDataContext context) {
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

    public void startBatch(JdbcDataContext context) {
    }

    public void writeData(Data data, JdbcDataContext ctx) {
        if (data.getEventType() == DataEventType.INSERT) {
            StatementBuilder st = getStatementBuilder(DmlType.INSERT, ctx, data);
            execute(ctx, st, data);
        }
        
        // check if an early commit needs to happen
    }
    
    protected int execute(JdbcDataContext ctx, StatementBuilder st, Data data) {
        Object[] objectValues = platform.getObjectValues(ctx.getBinaryEncoding(), data.toParsedRowData(), st
                .getMetaData(true));
        if (columnFilters != null) {
            for (IColumnFilter<DataContext> columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, ctx.getTargetTable(),
                        objectValues);
            }
        }
        return jdbcTemplate.update(st.getSql(), new ArgTypePreparedStatementSetter(objectValues, st
                .getTypes(), dbDialect.getLobHandler()));
    }    

    final private StatementBuilder getStatementBuilder(DmlType dmlType, JdbcDataContext ctx,
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

    public void close(JdbcDataContext context) {
        context.close();
    }

    public void finishBatch(JdbcDataContext context) {
        context.commit();
    }

}
