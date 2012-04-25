package org.jumpmind.symmetric.io.data.writer;

import java.io.StringReader;
import java.lang.reflect.Method;
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
import org.jumpmind.db.model.ModelException;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.util.Statistics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import bsh.EvalError;
import bsh.Interpreter;

public class DatabaseWriter implements IDataWriter {

    protected final static Logger log = LoggerFactory.getLogger(DatabaseWriter.class);

    public static enum LoadStatus {
        SUCCESS, CONFLICT
    };

    protected IDatabasePlatform platform;

    protected ISqlTransaction transaction;

    protected DmlStatement currentDmlStatement;

    protected Table sourceTable;

    protected Table targetTable;

    protected Map<String, Table> targetTables = new HashMap<String, Table>();

    protected CsvData lastData;

    protected Batch batch;

    protected DataContext context;

    protected long uncommittedCount = 0;

    protected DatabaseWriterSettings writerSettings;

    protected Map<Batch, Statistics> statistics = new HashMap<Batch, Statistics>();

    protected IDatabaseWriterConflictResolver conflictResolver;

    public DatabaseWriter(IDatabasePlatform platform) {
        this(platform, null, null);
    }

    public DatabaseWriter(IDatabasePlatform platform, DatabaseWriterSettings settings) {
        this(platform, null, settings);
    }

    public DatabaseWriter(IDatabasePlatform platform,
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        this.platform = platform;
        this.conflictResolver = conflictResolver == null ? new DefaultDatabaseWriterConflictResolver()
                : conflictResolver;
        this.writerSettings = settings == null ? new DatabaseWriterSettings() : settings;
    }

    public void open(DataContext context) {
        this.context = context;
        this.transaction = platform.getSqlTemplate().startSqlTransaction();
    }

    public void start(Batch batch) {
        this.batch = batch;
        this.statistics.put(batch, new Statistics());
    }

    public boolean start(Table table) {
        this.lastData = null;
        this.currentDmlStatement = null;
        this.sourceTable = table;
        this.targetTable = lookupTableAtTarget(table);
        if (this.targetTable != null || hasFilterThatHandlesMissingTable(table)) {
            this.transaction.allowInsertIntoAutoIncrementColumns(true, this.targetTable);
            return true;
        } else {
            log.warn("Did not find the {} table in the target database",
                    sourceTable.getFullyQualifiedTableName());
            return false;
        }
    }

    public void write(CsvData data) {
        try {
            statistics.get(batch).increment(DataWriterStatisticConstants.STATEMENTCOUNT);
            statistics.get(batch).increment(DataWriterStatisticConstants.LINENUMBER);
            if (filterBefore(data)) {
                LoadStatus loadStatus = LoadStatus.SUCCESS;
                switch (data.getDataEventType()) {
                    case UPDATE:
                        loadStatus = update(data, true, true);
                        break;
                    case INSERT:
                        loadStatus = insert(data);
                        break;
                    case DELETE:
                        loadStatus = delete(data, true);
                        break;
                    case BSH:
                        script(data);
                        break;
                    case SQL:
                        sql(data);
                        break;
                    case CREATE:
                        create(data);
                        break;
                    default:
                        break;
                }

                if (loadStatus == LoadStatus.CONFLICT) {
                    if (conflictResolver != null) {
                        conflictResolver.needsResolved(this, data, loadStatus);
                    } else {
                        throw new ConflictException(data, targetTable, false);
                    }
                } else {
                    uncommittedCount++;
                }

                lastData = data;

                filterAfter(data);

                if (uncommittedCount >= writerSettings.getMaxRowsBeforeCommit()) {
                    notifyFiltersEarlyCommit();
                    commit();
                }

            }

        } catch (IgnoreBatchException ex) {
            rollback();
            throw ex;
        }

    }

    protected void commit() {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                this.transaction.commit();
                notifyFiltersBatchCommitted();
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            }

        }
        uncommittedCount = 0;
    }

    protected void rollback() {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                this.transaction.rollback();
                notifyFiltersBatchRolledback();
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
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
        List<IDatabaseWriterFilter> filters = this.writerSettings.getDatabaseWriterFilters();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    process &= filter.beforeWrite(this.context,
                            this.targetTable != null ? this.targetTable : this.sourceTable, data);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
        return process;
    }

    protected void notifyFiltersEarlyCommit() {
        List<IDatabaseWriterFilter> filters = this.writerSettings.getDatabaseWriterFilters();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.earlyCommit(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
    }

    protected void notifyFiltersBatchComplete() {
        List<IDatabaseWriterFilter> filters = this.writerSettings.getDatabaseWriterFilters();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.batchComplete(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
    }

    protected void notifyFiltersBatchCommitted() {
        List<IDatabaseWriterFilter> filters = this.writerSettings.getDatabaseWriterFilters();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.batchCommitted(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
    }

    protected void notifyFiltersBatchRolledback() {
        List<IDatabaseWriterFilter> filters = this.writerSettings.getDatabaseWriterFilters();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.batchRolledback(context);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
    }

    protected void filterAfter(CsvData data) {
        List<IDatabaseWriterFilter> filters = this.writerSettings.getDatabaseWriterFilters();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterFilter filter : filters) {
                    filter.afterWrite(this.context, this.targetTable != null ? this.targetTable
                            : this.sourceTable, data);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
    }

    protected LoadStatus insert(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            if (requireNewStatement(DmlType.INSERT, data)) {
                this.currentDmlStatement = platform.createDmlStatement(DmlType.INSERT, targetTable);
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                String[] values = (String[]) ArrayUtils.addAll(getRowData(data),
                        getLookupKeyData(data));
                long count = execute(data, values);
                statistics.get(batch).increment(DataWriterStatisticConstants.INSERTCOUNT, count);
                return count > 0 ? LoadStatus.SUCCESS : LoadStatus.CONFLICT;
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)) {
                    return LoadStatus.CONFLICT;
                } else {
                    throw ex;
                }
            }
        } catch (SqlException ex) {
            logFailure(ex, data);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    protected String[] getRowData(CsvData data) {
        String[] targetValues = new String[targetTable.getColumnCount()];
        String[] originalValues = data.getParsedData(CsvData.ROW_DATA);
        String[] sourceColumnNames = sourceTable.getColumnNames();
        String[] targetColumnNames = targetTable.getColumnNames();
        for (int i = 0, t = 0; i < sourceColumnNames.length; i++) {
            if (sourceColumnNames[i].equalsIgnoreCase(targetColumnNames[t])) {
                targetValues[t] = originalValues[i];
                t++;
            }
        }
        return targetValues;
    }

    protected LoadStatus delete(CsvData data, boolean useConflictDetection) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            if (requireNewStatement(DmlType.DELETE, data)) {
                Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
                Column[] lookupKeys = null;
                if (!useConflictDetection) {
                    lookupKeys = targetTable.getPrimaryKeyColumns();
                } else {
                    switch (conflict.getDetectType()) {
                        case USE_OLD_DATA:
                            lookupKeys = targetTable.getColumns();
                            break;
                        case USE_VERSION:
                        case USE_TIMESTAMP:
                            List<Column> lookupColumns = new ArrayList<Column>();
                            Column versionColumn = targetTable.getColumnWithName(conflict
                                    .getDetectExpression());
                            if (versionColumn != null) {
                                lookupColumns.add(versionColumn);
                            } else {
                                log.error(
                                        "Could not find the timestamp/version column with the name {}.  Defaulting to using primary keys for the lookup.",
                                        conflict.getDetectExpression());
                            }
                            Column[] pks = targetTable.getPrimaryKeyColumns();
                            for (Column column : pks) {
                                // make sure all of the PK keys are in the list
                                // only
                                // once and are always at the end of the list
                                lookupColumns.remove(column);
                                lookupColumns.add(column);
                            }
                            lookupKeys = lookupColumns.toArray(new Column[lookupColumns.size()]);
                            break;
                        case USE_PK_DATA:
                        default:
                            lookupKeys = targetTable.getPrimaryKeyColumns();
                            break;
                    }
                }

                this.currentDmlStatement = platform.createDmlStatement(DmlType.DELETE,
                        targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                        lookupKeys, null);
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                long count = execute(data, getLookupKeyData(data));
                statistics.get(batch).increment(DataWriterStatisticConstants.DELETECOUNT, count);
                return count > 0 ? LoadStatus.SUCCESS : LoadStatus.CONFLICT;
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)) {
                    return LoadStatus.CONFLICT;
                } else {
                    throw ex;
                }
            }
        } catch (SqlException ex) {
            logFailure(ex, data);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }

    }

    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String[] columnValues = data.getParsedData(CsvData.ROW_DATA);
            ArrayList<String> changedColumnNameList = new ArrayList<String>();
            ArrayList<String> changedColumnValueList = new ArrayList<String>();
            ArrayList<Column> changedColumnMetaList = new ArrayList<Column>();
            for (int i = 0; i < columnValues.length; i++) {
                Column column = targetTable.getColumn(i);
                if (column != null) {
                    if (doesColumnNeedUpdated(i, column, data, applyChangesOnly)) {
                        changedColumnNameList.add(column.getName());
                        changedColumnMetaList.add(column);
                        changedColumnValueList.add(columnValues[i]);
                    }
                }
            }
            if (changedColumnNameList.size() > 0) {
                if (requireNewStatement(DmlType.UPDATE, data)) {
                    Column[] lookupKeys = null;
                    if (!useConflictDetection) {
                        lookupKeys = targetTable.getPrimaryKeyColumns();
                    } else {
                        Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
                        switch (conflict.getDetectType()) {
                            case USE_CHANGED_DATA:
                                ArrayList<Column> lookupColumns = new ArrayList<Column>(
                                        changedColumnMetaList);
                                Column[] pks = targetTable.getPrimaryKeyColumns();
                                for (Column column : pks) {
                                    // make sure all of the PK keys are in the
                                    // list only once and are always at the end
                                    // of the list
                                    lookupColumns.remove(column);
                                    lookupColumns.add(column);
                                }
                                lookupKeys = lookupColumns
                                        .toArray(new Column[lookupColumns.size()]);
                                break;
                            case USE_OLD_DATA:
                                lookupKeys = targetTable.getColumns();
                                break;
                            case USE_VERSION:
                            case USE_TIMESTAMP:
                                lookupColumns = new ArrayList<Column>();
                                Column versionColumn = targetTable.getColumnWithName(conflict
                                        .getDetectExpression());
                                if (versionColumn != null) {
                                    lookupColumns.add(versionColumn);
                                } else {
                                    log.error(
                                            "Could not find the timestamp/version column with the name {}.  Defaulting to using primary keys for the lookup.",
                                            conflict.getDetectExpression());
                                }
                                pks = targetTable.getPrimaryKeyColumns();
                                for (Column column : pks) {
                                    // make sure all of the PK keys are in the
                                    // list only once and are always at the end
                                    // of the list
                                    lookupColumns.remove(column);
                                    lookupColumns.add(column);
                                }
                                lookupKeys = lookupColumns
                                        .toArray(new Column[lookupColumns.size()]);
                                break;
                            case USE_PK_DATA:
                            default:
                                lookupKeys = targetTable.getPrimaryKeyColumns();
                                break;
                        }
                    }
                    this.currentDmlStatement = platform
                            .createDmlStatement(DmlType.UPDATE, targetTable.getCatalog(),
                                    targetTable.getSchema(), targetTable.getName(), lookupKeys,
                                    changedColumnMetaList.toArray(new Column[changedColumnMetaList
                                            .size()]));
                    transaction.prepare(this.currentDmlStatement.getSql());

                }
                columnValues = (String[]) changedColumnValueList
                        .toArray(new String[changedColumnValueList.size()]);
                String[] values = (String[]) ArrayUtils
                        .addAll(columnValues, getLookupKeyData(data));
                try {
                    long count = execute(data, values);
                    statistics.get(batch)
                            .increment(DataWriterStatisticConstants.UPDATECOUNT, count);
                    return count > 0 ? LoadStatus.SUCCESS : LoadStatus.CONFLICT;
                } catch (SqlException ex) {
                    if (platform.getSqlTemplate().isUniqueKeyViolation(ex)) {
                        return LoadStatus.CONFLICT;
                    } else {
                        throw ex;
                    }
                }
            } else {
                // There was no change to apply
                return LoadStatus.SUCCESS;
            }
        } catch (SqlException ex) {
            logFailure(ex, data);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    protected void logFailure(SqlException e, CsvData data) {
        StringBuilder failureMessage = new StringBuilder();
        failureMessage.append("Failed to process a ");
        failureMessage.append(data.getDataEventType().toString().toLowerCase());
        failureMessage.append(" event in batch ");
        failureMessage.append(batch.getBatchId());
        failureMessage.append(".\n");
        if (this.currentDmlStatement != null) {
            failureMessage.append("Failed sql was: ");
            failureMessage.append(this.currentDmlStatement.getSql());
            failureMessage.append("\n");
        }
        String rowData = data.getCsvData(CsvData.PK_DATA);
        if (StringUtils.isNotBlank(rowData)) {
            failureMessage.append("Failed pk data was: ");
            failureMessage.append(rowData);
            failureMessage.append("\n");
        }

        rowData = data.getCsvData(CsvData.ROW_DATA);
        if (StringUtils.isNotBlank(rowData)) {
            failureMessage.append("Failed row data was: ");
            failureMessage.append(rowData);
            failureMessage.append("\n");
        }

        rowData = data.getCsvData(CsvData.OLD_DATA);
        if (StringUtils.isNotBlank(rowData)) {
            failureMessage.append("Failed old data was: ");
            failureMessage.append(rowData);
            failureMessage.append("\n");
        }

        log.error(failureMessage.toString());

    }

    protected boolean script(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String script = data.getCsvData(CsvData.ROW_DATA);
            Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("SOURCE_NODE_ID", batch.getNodeId());
            ISqlTemplate template = platform.getSqlTemplate();
            Class<?> templateClass = template.getClass();
            if (templateClass.getSimpleName().equals("JdbcSqlTemplate")) {
                try {
                    Method method = templateClass.getMethod("getDataSource");
                    variables.put("DATASOURCE", method.invoke(template));
                } catch (Exception e) {
                    log.warn("Had trouble looking up the datasource used by the sql template", e);
                }
            }

            Interpreter interpreter = new Interpreter();
            if (variables != null) {
                for (String variableName : variables.keySet()) {
                    interpreter.set(variableName, variables.get(variableName));
                }
            }

            log.info("About to run: {}", script);
            interpreter.eval(script);
            statistics.get(batch).increment(DataWriterStatisticConstants.SCRIPTCOUNT);
        } catch (EvalError e) {
            throw new RuntimeException(e);
        }
        return true;
    }

    protected boolean create(CsvData data) {
        try {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                String xml = data.getCsvData(CsvData.ROW_DATA);
                if (log.isDebugEnabled()) {
                    log.debug("About to create table using the following definition: ", xml);
                }
                StringReader reader = new StringReader(xml);
                Database db = (Database) new DatabaseIO().read(reader);
                platform.alterTables(false, db.getTables());
                platform.resetCachedTableModel();
                statistics.get(batch).increment(DataWriterStatisticConstants.CREATECOUNT);
                return true;
            } catch (Exception e) {
                throw new ModelException(e);
            }

        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }

    }

    protected boolean sql(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String sql = data.getCsvData(CsvData.ROW_DATA);
            transaction.prepare(sql);
            log.info("About to run: {}", sql);
            long count = transaction.prepareAndExecute(sql);
            log.info("{} rows updated when running: {}", count, sql);
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLCOUNT);
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLROWSAFFECTEDCOUNT,
                    count);
            return true;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }

    }

    protected boolean doesColumnNeedUpdated(int columnIndex, Column column, CsvData data,
            boolean applyChangesOnly) {
        boolean needsUpdated = true;
        String[] oldData = data.getParsedData(CsvData.OLD_DATA);
        String[] rowData = data.getParsedData(CsvData.ROW_DATA);
        if (!platform.getDatabaseInfo().isAutoIncrementUpdateAllowed() && column.isAutoIncrement()) {
            needsUpdated = false;
        } else if (oldData != null && applyChangesOnly) {
            needsUpdated = !StringUtils.equals(rowData[columnIndex], oldData[columnIndex])
                    || (platform.isLob(column.getTypeCode()) && StringUtils
                            .isBlank(oldData[columnIndex]));
        } else {
            // This is in support of creating update statements that don't use
            // the keys in the set portion of the update statement. </p> In
            // oracle (and maybe not only in oracle) if there is no index on
            // child table on FK column and update is performing on PK on master
            // table, table lock is acquired on child table. Table lock is taken
            // not in exclusive mode, but lock contentions is possible.
            needsUpdated = !column.isPrimaryKey()
                    || !StringUtils.equals(rowData[columnIndex], getPkDataFor(data, column));
        }
        return needsUpdated;
    }

    protected String[] getLookupKeyData(CsvData data) {
        Column[] keys = this.currentDmlStatement.getKeys();
        if (keys != null && keys.length > 0) {
            boolean allPks = Table.areAllColumnsPrimaryKeys(keys);
            if (allPks) {
                String[] keyDataAsArray = data.getParsedData(CsvData.PK_DATA);
                if (keyDataAsArray != null
                        && keyDataAsArray.length <= targetTable.getPrimaryKeyColumnCount()) {
                    return keyDataAsArray;
                }
            }

            Map<String, String> keyData = data
                    .toColumnNameValuePairs(targetTable, CsvData.OLD_DATA);
            if (keyData == null || keyData.size() == 0) {
                keyData = data.toColumnNameValuePairs(targetTable, CsvData.ROW_DATA);
            }

            if (keyData != null && keyData.size() > 0) {
                String[] keyDataAsArray = new String[keys.length];
                int index = 0;
                for (Column keyColumn : keys) {
                    keyDataAsArray[index++] = keyData.get(keyColumn.getName());
                }

                return keyDataAsArray;
            }
        }

        return null;

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
        if (writerSettings.getDatabaseWriterFilters() != null) {
            for (IDatabaseWriterFilter filter : writerSettings.getDatabaseWriterFilters()) {
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
                        this.writerSettings.isUsePrimaryKeysFromSource());

                boolean setAllColumnsAsPrimaryKey = table.getPrimaryKeyColumnCount() == 0;

                if (StringUtils.isBlank(sourceTable.getCatalog())) {
                    table.setCatalog(null);
                }

                if (StringUtils.isBlank(sourceTable.getSchema())) {
                    table.setSchema(null);
                }

                Column[] columns = table.getColumns();
                for (Column column : columns) {
                    int typeCode = column.getTypeCode();
                    if (this.writerSettings.isTreatDateTimeFieldsAsVarchar()
                            && (typeCode == Types.DATE || typeCode == Types.TIME || typeCode == Types.TIMESTAMP)) {
                        column.setTypeCode(Types.VARCHAR);
                    }

                    if (setAllColumnsAsPrimaryKey) {
                        column.setPrimaryKey(true);
                    }
                }
            }
        }
        return table;
    }

    public Batch getBatch() {
        return batch;
    }

    public DataContext getContext() {
        return context;
    }

    public IDatabaseWriterConflictResolver getConflictResolver() {
        return conflictResolver;
    }

    public void setConflictResolver(IDatabaseWriterConflictResolver conflictResolver) {
        this.conflictResolver = conflictResolver;
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

    public IDatabasePlatform getPlatform() {
        return platform;
    }

    public DatabaseWriterSettings getWriterSettings() {
        return writerSettings;
    }

}
