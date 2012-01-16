package org.jumpmind.symmetric.io.data.writer;

import java.io.StringReader;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseIO;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.jdbc.JdbcSqlTemplate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.ConflictException;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataReader;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;

import bsh.EvalError;
import bsh.Interpreter;

public class DatabaseWriter implements IDataWriter {

    protected final static Logger log = LoggerFactory.getLogger(DatabaseWriter.class);

    protected IDatabasePlatform platform;

    protected ISqlTransaction transaction;

    protected DmlStatement currentDmlStatement;

    protected Table sourceTable;

    protected Table targetTable;

    protected Map<String, Table> targetTables = new HashMap<String, Table>();

    protected CsvData lastData;

    protected Batch batch;

    protected DataContext<? extends IDataReader, ? extends IDataWriter> context;

    protected long uncommittedCount = 0;

    protected DatabaseWriterSettings defaultSettings;

    protected DatabaseWriterSettings batchSettings;

    protected List<IDatabaseWriterFilter> filters;

    protected Map<String, DatabaseWriterSettings> channelSpecificSettings;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected IDatabaseWriterConflictResolver conflictResolver;

    public DatabaseWriter(IDatabasePlatform platform) {
        this(platform, null);
    }

    public DatabaseWriter(IDatabasePlatform platform, DatabaseWriterSettings defaultSettings,
            IDatabaseWriterFilter... filters) {
        this(platform, null, defaultSettings, null, filters);
    }

    public DatabaseWriter(IDatabasePlatform platform, DatabaseWriterSettings defaultSettings,
            Map<String, DatabaseWriterSettings> channelSpecificSettings,
            IDatabaseWriterFilter... filters) {
        this(platform, null, defaultSettings, channelSpecificSettings, filters);
    }

    public DatabaseWriter(IDatabasePlatform platform,
            IDatabaseWriterConflictResolver conflictResolver,
            DatabaseWriterSettings defaultSettings,
            Map<String, DatabaseWriterSettings> channelSpecificSettings, 
            IDatabaseWriterFilter... filters) {
        this.platform = platform;
        this.conflictResolver = conflictResolver == null ? new DefaultDatabaseWriterConflictResolver()
                : conflictResolver;
        this.defaultSettings = defaultSettings == null ? new DatabaseWriterSettings()
                : defaultSettings;
        this.channelSpecificSettings = channelSpecificSettings == null ? new HashMap<String, DatabaseWriterSettings>()
                : channelSpecificSettings;
        this.filters = new ArrayList<IDatabaseWriterFilter>();
        if (filters != null) {
            for (IDatabaseWriterFilter filter : filters) {
                this.filters.add(filter);
            }
        }
    }

    public <R extends IDataReader, W extends IDataWriter> void open(DataContext<R, W> context) {
        this.context = context;
        this.transaction = platform.getSqlTemplate().startSqlTransaction();
    }

    public void start(Batch batch) {
        this.batch = batch;
        this.batchSettings = channelSpecificSettings.get(batch.getChannelId());
        if (this.batchSettings == null) {
            this.batchSettings = this.defaultSettings;
        }
        this.statistics.put(batch, new DatabaseWriterStatistics());
    }

    public boolean start(Table table) {
        this.lastData = null;
        this.currentDmlStatement = null;
        this.sourceTable = table;
        this.targetTable = lookupTableAtTarget(table);
        if (this.targetTable != null || hasFilterThatHandlesMissingTable(table)) {
            return true;
        } else {
            log.warn("Did not find the {} table in the target database",
                    sourceTable.getFullyQualifiedTableName());
            return false;
        }
    }

    public void write(CsvData data) {
        if (filterBefore(data)) {
            boolean success = false;
            switch (data.getDataEventType()) {
            case UPDATE:
                statistics.get(batch).increment(DatabaseWriterStatistics.STATEMENTCOUNT);
                success = update(data);
                break;
            case INSERT:
                statistics.get(batch).increment(DatabaseWriterStatistics.STATEMENTCOUNT);
                success = insert(data);
                break;
            case DELETE:
                statistics.get(batch).increment(DatabaseWriterStatistics.STATEMENTCOUNT);
                success = delete(data);
                break;
            case BSH:
                success = script(data);
                break;
            case SQL:
                success = sql(data);
                break;
            case CREATE:
                success = create(data);
                break;
            default:
                success = true;
                break;
            }

            if (!success) {
                if (conflictResolver != null) {
                    conflictResolver.needsResolved(this, data);
                } else {
                    throw new ConflictException(data, targetTable, false);
                }
            } else {
                uncommittedCount++;
            }

            lastData = data;

            filterAfter(data);

            if (uncommittedCount >= batchSettings.getMaxRowsBeforeCommit()) {
                notifyFiltersEarlyCommit();
                commit();
            }

        }

    }

    protected void commit() {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
                this.transaction.commit();
                notifyFiltersBatchCommitted();
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            }

        }
        uncommittedCount = 0;
    }

    protected void rollback() {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
                this.transaction.rollback();
                notifyFiltersBatchRolledback();
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            }

        }
        uncommittedCount = 0;
    }

    protected boolean requireNewStatement(DmlType currentType, CsvData data) {
        boolean requiresNew = currentDmlStatement == null || lastData == null
                || currentDmlStatement.getDmlType() != currentType
                || lastData.getDataEventType() != data.getDataEventType();
        if (!requiresNew && data.getDataEventType() == DataEventType.UPDATE) {
            String currentChanges = Arrays.toString(data.getChangedDataIndicators());
            String lastChanges = Arrays.toString(lastData.getChangedDataIndicators());
            requiresNew = !currentChanges.equals(lastChanges);
        }
        return requiresNew;
    }

    protected boolean filterBefore(CsvData data) {
        boolean process = true;
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    process &= filter.beforeWrite(this.context,
                            this.targetTable != null ? this.targetTable : this.sourceTable, data);
                }
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.FILTERMILLIS);
            }
        }
        return process;
    }

    protected void notifyFiltersEarlyCommit() {
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.earlyCommit(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.FILTERMILLIS);
            }
        }
    }

    protected void notifyFiltersBatchComplete() {
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.batchComplete(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.FILTERMILLIS);
            }
        }
    }

    protected void notifyFiltersBatchCommitted() {
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.batchCommitted(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.FILTERMILLIS);
            }
        }
    }

    protected void notifyFiltersBatchRolledback() {
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.batchRolledback(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.FILTERMILLIS);
            }
        }
    }

    protected void filterAfter(CsvData data) {
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DatabaseWriterStatistics.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.afterWrite(this.context, this.targetTable != null ? this.targetTable
                            : this.sourceTable, data);
                }
            } finally {
                statistics.get(batch).stopTimer(DatabaseWriterStatistics.FILTERMILLIS);
            }
        }
    }

    protected boolean insert(CsvData data) {
        try {
            statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            if (requireNewStatement(DmlType.INSERT, data)) {
                this.currentDmlStatement = platform.createDmlStatement(DmlType.INSERT, targetTable);
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                String[] values = (String[]) ArrayUtils.addAll(
                        data.getParsedData(CsvData.ROW_DATA), getPkData(data));
                long count = execute(data, values);
                statistics.get(batch).increment(DatabaseWriterStatistics.INSERTCOUNT, count);
                return count > 0;
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)) {
                    return false;
                } else {
                    throw ex;
                }
            }
        } finally {
            statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
        }

    }

    protected boolean delete(CsvData data) {
        try {
            statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            if (requireNewStatement(DmlType.DELETE, data)) {
                this.currentDmlStatement = platform.createDmlStatement(DmlType.DELETE, targetTable);
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            long count = execute(data, getPkData(data));
            statistics.get(batch).increment(DatabaseWriterStatistics.DELETECOUNT, count);
            return count > 0;
        } finally {
            statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
        }

    }

    protected boolean update(CsvData data) {
        try {
            statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            String[] columnValues = data.getParsedData(CsvData.ROW_DATA);
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
                if (requireNewStatement(DmlType.UPDATE, data)) {
                    this.currentDmlStatement = platform
                            .createDmlStatement(
                                    DmlType.UPDATE,
                                    targetTable.getCatalog(),
                                    targetTable.getSchema(),
                                    targetTable.getName(),
                                    batchSettings.isUseAllColumnsToIdentifyUpdateConflicts() ? targetTable
                                            .getColumns() : targetTable.getPrimaryKeyColumns(),
                                    changedColumnMetaList.toArray(new Column[changedColumnMetaList
                                            .size()]));
                    transaction.prepare(this.currentDmlStatement.getSql());

                }
                columnValues = (String[]) changedColumnValueList
                        .toArray(new String[changedColumnValueList.size()]);
                String[] values = (String[]) ArrayUtils.addAll(columnValues, getPkData(data));
                long count = execute(data, values);
                statistics.get(batch).increment(DatabaseWriterStatistics.UPDATECOUNT, count);
                return count > 0;
            } else {
                // There was no change to apply
                return true;
            }
        } finally {
            statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
        }
    }

    protected boolean script(CsvData data) {
        try {
            statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            String script = data.getCsvData(CsvData.ROW_DATA);
            Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("SOURCE_NODE_ID", batch.getSourceNodeId());
            if (platform.getSqlTemplate() instanceof JdbcSqlTemplate) {
                variables.put("DATASOURCE",
                        ((JdbcSqlTemplate) platform.getSqlTemplate()).getDataSource());
            }

            Interpreter interpreter = new Interpreter();
            if (variables != null) {
                for (String variableName : variables.keySet()) {
                    interpreter.set(variableName, variables.get(variableName));
                }
            }

            log.info("About to run: {}", script);
            interpreter.eval(script);
            statistics.get(batch).increment(DatabaseWriterStatistics.SCRIPTCOUNT);
        } catch (EvalError e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    protected boolean create(CsvData data) {
        try {
            statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            String xml = data.getCsvData(CsvData.ROW_DATA);
            if (log.isDebugEnabled()) {
                log.debug("About to create table using the following definition: ", xml);
            }
            StringReader reader = new StringReader(xml);
            Database db = new DatabaseIO().read(reader);
            platform.alterTables(false, db.getTables());
            platform.resetCachedTableModel();
            statistics.get(batch).increment(DatabaseWriterStatistics.CREATECOUNT);
            return true;
        } finally {
            statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
        }

    }

    protected boolean sql(CsvData data) {
        try {
            statistics.get(batch).startTimer(DatabaseWriterStatistics.DATABASEMILLIS);
            String sql = data.getCsvData(CsvData.ROW_DATA);
            transaction.prepare(sql);
            log.info("About to run: {}", sql);
            long count = transaction.execute(sql);
            log.info("{} rows updated when running: {}", count, sql);
            statistics.get(batch).increment(DatabaseWriterStatistics.SQLCOUNT);
            statistics.get(batch).increment(DatabaseWriterStatistics.SQLROWSAFFECTEDCOUNT, count);
            return true;
        } finally {
            statistics.get(batch).stopTimer(DatabaseWriterStatistics.DATABASEMILLIS);
        }

    }

    protected boolean doesColumnNeedUpdated(int columnIndex, Column column, CsvData data) {
        boolean needsUpdated = true;
        String[] oldData = data.getParsedData(CsvData.OLD_DATA);
        String[] rowData = data.getParsedData(CsvData.ROW_DATA);
        if (!platform.getPlatformInfo().isAutoIncrementUpdateAllowed() && column.isAutoIncrement()) {
            needsUpdated = false;
        } else if (oldData != null) {
            needsUpdated = !StringUtils.equals(rowData[columnIndex], oldData[columnIndex])
                    || (platform.isLob(column.getTypeCode()) && StringUtils
                            .isBlank(oldData[columnIndex]));
        } else if (batchSettings.dontIncludeKeysInUpdateStatement) {
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

    protected String[] getPkData(CsvData data) {
        String[] pkData = null;
        if (batchSettings.isUseAllColumnsToIdentifyUpdateConflicts()) {
            pkData = data.getParsedData(CsvData.OLD_DATA);
            if (pkData == null) {
                pkData = data.getParsedData(CsvData.ROW_DATA);
            }
        } else {
            pkData = data.getParsedData(CsvData.PK_DATA);
            if (pkData == null || pkData.length < targetTable.getPrimaryKeyColumnCount()) {
                String[] values = data.getParsedData(CsvData.OLD_DATA);
                if (values == null) {
                    values = data.getParsedData(CsvData.ROW_DATA);
                }
                Column[] pkColumns = targetTable.getPrimaryKeyColumns();
                pkData = new String[pkColumns.length];
                int i = 0;
                for (Column column : pkColumns) {
                    pkData[i++] = values[targetTable.getColumnIndex(column)];
                }
            }
        }
        return pkData;
    }

    protected String getPkDataFor(CsvData data, Column column) {
        String[] values = data.getParsedData(CsvData.PK_DATA);
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
            return data.getParsedData(CsvData.ROW_DATA)[targetTable.getColumnIndex(column)];
        }
        return null;
    }

    protected int execute(CsvData data, String[] values) {
        Object[] objectValues = platform.getObjectValues(batch.getBinaryEncoding(), values,
                currentDmlStatement.getMetaData());
        return transaction.addRow(data, objectValues, this.currentDmlStatement.getTypes());
    }

    public void end(Table table) {
        this.transaction.allowInsertIntoAutoIncrementColumns(false, this.targetTable);
    }

    public void end(Batch batch, boolean inError) {
        this.lastData = null;
        this.currentDmlStatement = null;
        if (!inError) {
            notifyFiltersBatchComplete();
            commit();
        } else {
            rollback();
        }
    }

    public void close() {
        if (transaction != null) {
            this.transaction.close();
        }
    }

    protected boolean hasFilterThatHandlesMissingTable(Table table) {
        if (filters != null) {
            for (IDatabaseWriterFilter filter : filters) {
                if (filter.handlesMissingTable(context, table)) {
                    return true;
                }
            }
        }
        return false;
    }

    protected Table lookupTableAtTarget(Table sourceTable) {
        String tableNameKey = sourceTable.getFullyQualifiedTableName();
        Table table = targetTables.get(tableNameKey);
        if (table == null) {
            table = platform.getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), false);
            if (table != null) {
                table = table.copy();
                table.reOrderColumns(sourceTable.getColumns(),
                        this.batchSettings.isUsePrimaryKeysFromSource());

                boolean setAllColumnsAsPrimaryKey = table.getPrimaryKeyColumnCount() == 0;

                Column[] columns = table.getColumns();
                for (Column column : columns) {
                    int typeCode = column.getTypeCode();
                    if (this.batchSettings.isTreatDateTimeFieldsAsVarchar()
                            && (typeCode == Types.DATE || typeCode == Types.TIME || typeCode == Types.TIMESTAMP)) {
                        column.setTypeCode(Types.VARCHAR);
                    }

                    if (setAllColumnsAsPrimaryKey) {
                        column.setPrimaryKey(true);
                    }
                }
                this.transaction.allowInsertIntoAutoIncrementColumns(true, this.targetTable);
            }
        }
        return table;
    }

    public Batch getBatch() {
        return batch;
    }

    public DataContext<? extends IDataReader, ? extends IDataWriter> getContext() {
        return context;
    }

    public IDatabaseWriterConflictResolver getConflictResolver() {
        return conflictResolver;
    }

    public void setConflictResolver(IDatabaseWriterConflictResolver conflictResolver) {
        this.conflictResolver = conflictResolver;
    }

    public DatabaseWriterSettings getTargetTableSettings() {
        return batchSettings;
    }

    public DmlStatement getCurrentDmlStatement() {
        return currentDmlStatement;
    }

    public Table getTargetTable() {
        return targetTable;
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    public ISqlTransaction getTransaction() {
        return transaction;
    }

}
