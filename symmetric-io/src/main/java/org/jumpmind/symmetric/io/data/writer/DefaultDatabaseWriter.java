/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.io.data.writer;

import java.io.StringReader;
import java.lang.reflect.Method;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.DatabaseInfo;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlScriptReader;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectExpressionKey;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.FormatUtils;
import org.jumpmind.util.LogSuppressor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultDatabaseWriter extends AbstractDatabaseWriter {
    
    protected final static Logger log = LoggerFactory.getLogger(DefaultDatabaseWriter.class);
    
    public static final String CUR_DATA = "DatabaseWriter.CurData";

    protected IDatabasePlatform platform;

    protected ISqlTransaction transaction;

    protected DmlStatement currentDmlStatement;
    
    protected Object[] currentDmlValues;
    
    protected static LogSuppressor logSuppressor = new LogSuppressor(log, 1);

    public DefaultDatabaseWriter(IDatabasePlatform platform) {
        this(platform, null, null);
    }

    public DefaultDatabaseWriter(IDatabasePlatform platform, DatabaseWriterSettings settings) {
        this(platform, null, settings);
    }

    public DefaultDatabaseWriter(IDatabasePlatform platform,
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        super(conflictResolver, settings);
        this.platform = platform;
    }

    @Override
    public void open(DataContext context) {
        super.open(context);
        this.transaction = platform.getSqlTemplate().startSqlTransaction();
    }

    @Override
    public boolean start(Table table) {
        this.currentDmlStatement = null;
        boolean process = super.start(table);
        if (process && targetTable != null) {
            allowInsertIntoAutoIncrementColumns(true, this.targetTable);
        } 
        return process;
    }    

    @Override
    public void end(Table table) {
        super.end(table);
        allowInsertIntoAutoIncrementColumns(false, this.targetTable);
    }

    @Override
    public void end(Batch batch, boolean inError) {
        this.currentDmlStatement = null;
        super.end(batch, inError);
    }

    @Override
    public void close() {
        super.close();
        if (transaction != null) {
            this.transaction.close();
        }
    }

    @Override
    protected void commit(boolean earlyCommit) {
        if (transaction != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
                this.transaction.commit();
                if (!earlyCommit) {
                   notifyFiltersBatchCommitted();
                } else {
                    notifyFiltersEarlyCommit();
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            }

        }
        super.commit(earlyCommit);
    }

    @Override
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
        super.rollback();
    }

    @Override
    protected LoadStatus insert(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            if (requireNewStatement(DmlType.INSERT, data, false, true, null)) {
                this.lastUseConflictDetection = true;
                this.currentDmlStatement = platform.createDmlStatement(DmlType.INSERT, targetTable, writerSettings.getTextColumnExpression());
                if (log.isDebugEnabled()) {
                    log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                }
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
                String[] values = (String[]) ArrayUtils.addAll(getRowData(data, CsvData.ROW_DATA),
                        this.currentDmlStatement.getLookupKeyData(getLookupDataMap(data, conflict)));
                long count = execute(data, values);
                statistics.get(batch).increment(DataWriterStatisticConstants.INSERTCOUNT, count);
                if (count > 0) {
                        return LoadStatus.SUCCESS;
                } else {
                    context.put(CUR_DATA,getCurData(transaction));
                    return LoadStatus.CONFLICT;
                }
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)) {
                    if (!platform.getDatabaseInfo().isRequiresSavePointsInTransaction()) {
                        context.put(CONFLICT_ERROR, ex);
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    } else {
                        log.info("Detected a conflict via an exception, but cannot perform conflict resolution because the database in use requires savepoints");
                        throw ex;
                    }
                } else {
                    throw ex;
                }
            }
        } catch (SqlException ex) {
            logFailureDetails(ex, data, true);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    @Override
    protected LoadStatus delete(CsvData data, boolean useConflictDetection) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
            Map<String, String> lookupDataMap = null;
            if (requireNewStatement(DmlType.DELETE, data, useConflictDetection, useConflictDetection,
                    conflict.getDetectType())) {
                this.lastUseConflictDetection = useConflictDetection;
                List<Column> lookupKeys = null;
                if (!useConflictDetection) {
                    lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                } else {
                    switch (conflict.getDetectType()) {
                        case USE_OLD_DATA:
                            lookupKeys = targetTable.getColumnsAsList();
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
                                // only once and are always at the end of the
                                // list
                                lookupColumns.remove(column);
                                lookupColumns.add(column);
                            }
                            lookupKeys = lookupColumns;
                            break;
                        case USE_PK_DATA:
                        default:
                            lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                            break;
                    }
                }

                if (lookupKeys == null || lookupKeys.size() == 0) {
                    lookupKeys = targetTable.getColumnsAsList();
                }

                int lookupKeyCountBeforeColumnRemoval = lookupKeys.size();

                Iterator<Column> it = lookupKeys.iterator();
                while (it.hasNext()) {
                    Column col = it.next();
                    if ((platform.isLob(col.getMappedTypeCode()) && data.isNoBinaryOldData())
                            || !platform.canColumnBeUsedInWhereClause(col)) {
                        it.remove();
                    }
                }
                
                if (lookupKeys.size() == 0) {
                    String msg = "There are no keys defined for "
                            + targetTable.getFullyQualifiedTableName()
                            + ".  Cannot build an update statement.  ";
                    if (lookupKeyCountBeforeColumnRemoval > 0) {
                        msg += "The only keys defined are binary and they have been removed.";
                    }
                    throw new IllegalStateException(msg);                        
                }

                lookupDataMap = getLookupDataMap(data, conflict);

                boolean[] nullKeyValues = new boolean[lookupKeys.size()];
                for (int i = 0; i < lookupKeys.size(); i++) {
                    Column column = lookupKeys.get(i);
                    nullKeyValues[i] = !column.isRequired()
                            && lookupDataMap.get(column.getName()) == null;
                }

                this.currentDmlStatement = platform.createDmlStatement(DmlType.DELETE,
                        targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                        lookupKeys.toArray(new Column[lookupKeys.size()]), null, nullKeyValues, writerSettings.getTextColumnExpression());
                if (log.isDebugEnabled()) {
                    log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                }
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                lookupDataMap = lookupDataMap == null ? getLookupDataMap(data, conflict) : lookupDataMap;
                long count = execute(data, this.currentDmlStatement.getLookupKeyData(lookupDataMap));
                statistics.get(batch).increment(DataWriterStatisticConstants.DELETECOUNT, count);
                if (count > 0) {
                        return LoadStatus.SUCCESS;
                } else {
                    context.put(CUR_DATA,null); // since a delete conflicted, there's no row to delete, so no cur data.
                    return LoadStatus.CONFLICT;
                }
            } catch (SqlException ex) {
                if (platform.getSqlTemplate().isUniqueKeyViolation(ex)
                        && !platform.getDatabaseInfo().isRequiresSavePointsInTransaction()) {
                    context.put(CUR_DATA,null); // since a delete conflicted, there's no row to delete, so no cur data.
                    return LoadStatus.CONFLICT;
                } else {
                    throw ex;
                }
            }
        } catch (SqlException ex) {
            logFailureDetails(ex, data, true);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }

    }

    @Override
    protected LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String[] rowData = getRowData(data, CsvData.ROW_DATA);
            String[] oldData = getRowData(data, CsvData.OLD_DATA);
            ArrayList<String> changedColumnNameList = new ArrayList<String>();
            ArrayList<String> changedColumnValueList = new ArrayList<String>();
            ArrayList<Column> changedColumnsList = new ArrayList<Column>();
            for (int i = 0; i < targetTable.getColumnCount(); i++) {
                Column column = targetTable.getColumn(i);
                if (column != null) {
                    if (doesColumnNeedUpdated(i, column, data, rowData, oldData, applyChangesOnly)) {
                        changedColumnNameList.add(column.getName());
                        changedColumnsList.add(column);
                        changedColumnValueList.add(rowData[i]);
                    }
                }
            }

            if (changedColumnNameList.size() > 0) {
                Map<String, String> lookupDataMap = null;
                Conflict conflict = writerSettings.pickConflict(this.targetTable, batch);
                if (requireNewStatement(DmlType.UPDATE, data, applyChangesOnly,
                        useConflictDetection, conflict.getDetectType())) {
                    lastApplyChangesOnly = applyChangesOnly;
                    lastUseConflictDetection = useConflictDetection;
                    List<Column> lookupKeys = null;
                    if (!useConflictDetection) {
                        lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                    } else {
                        switch (conflict.getDetectType()) {
                            case USE_CHANGED_DATA:
                                ArrayList<Column> lookupColumns = new ArrayList<Column>(
                                        changedColumnsList);
                                Column[] pks = targetTable.getPrimaryKeyColumns();
                                for (Column column : pks) {
                                    // make sure all of the PK keys are in the
                                    // list only once and are always at the end
                                    // of the list
                                    lookupColumns.remove(column);
                                    lookupColumns.add(column);
                                }
                                removeExcludedColumns(conflict, lookupColumns);
                                lookupKeys = lookupColumns;
                                break;
                            case USE_OLD_DATA:
                                lookupKeys = targetTable.getColumnsAsList();
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
                                lookupKeys = lookupColumns;
                                break;
                            case USE_PK_DATA:
                            default:
                                lookupKeys = targetTable.getPrimaryKeyColumnsAsList();
                                break;
                        }
                    }

                    if (lookupKeys == null || lookupKeys.size() == 0) {
                        lookupKeys = targetTable.getColumnsAsList();
                    }

                    int lookupKeyCountBeforeColumnRemoval = lookupKeys.size();
                    
                    Iterator<Column> it = lookupKeys.iterator();
                    while (it.hasNext()) {
                        Column col = it.next();
                        if ((platform.isLob(col.getMappedTypeCode()) && data.isNoBinaryOldData())
                                || !platform.canColumnBeUsedInWhereClause(col)) {
                            it.remove();
                        }
                    }
                    
                    if (lookupKeys.size() == 0) {
                        String msg = "There are no keys defined for "
                                + targetTable.getFullyQualifiedTableName()
                                + ".  Cannot build an update statement.  ";
                        if (lookupKeyCountBeforeColumnRemoval > 0) {
                            msg += "The only keys defined are binary and they have been removed.";
                        }
                        throw new IllegalStateException(msg);                        
                    }

                    lookupDataMap = getLookupDataMap(data, conflict);

                    boolean[] nullKeyValues = new boolean[lookupKeys.size()];
                    for (int i = 0; i < lookupKeys.size(); i++) {
                        Column column = lookupKeys.get(i);
                        // the isRequired is a bit of a hack. This nullKeyValues
                        // should really be checking against the object values
                        // because some null values get translated into empty
                        // strings
                        nullKeyValues[i] = !column.isRequired()
                                && lookupDataMap.get(column.getName()) == null;
                    }

                    this.currentDmlStatement = platform.createDmlStatement(DmlType.UPDATE,
                            targetTable.getCatalog(), targetTable.getSchema(),
                            targetTable.getName(),
                            lookupKeys.toArray(new Column[lookupKeys.size()]),
                            changedColumnsList.toArray(new Column[changedColumnsList.size()]),
                            nullKeyValues, writerSettings.getTextColumnExpression());
                    if (log.isDebugEnabled()) {
                        log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                    }
                    transaction.prepare(this.currentDmlStatement.getSql());

                }

                rowData = (String[]) changedColumnValueList
                        .toArray(new String[changedColumnValueList.size()]);
                lookupDataMap = lookupDataMap == null ? getLookupDataMap(data, conflict) : lookupDataMap;
                String[] values = (String[]) ArrayUtils.addAll(rowData,
                        this.currentDmlStatement.getLookupKeyData(lookupDataMap));

                try {
                    long count = execute(data, values);
                    statistics.get(batch)
                            .increment(DataWriterStatisticConstants.UPDATECOUNT, count);
                    if (count > 0) {
                        return LoadStatus.SUCCESS;
                    } else {
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    }
                } catch (SqlException ex) {
                    if (platform.getSqlTemplate().isUniqueKeyViolation(ex)
                            && !platform.getDatabaseInfo().isRequiresSavePointsInTransaction()) {
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    } else {
                        throw ex;
                    }
                }
            } else {
                if (log.isDebugEnabled()) {
                   log.debug("Not running update for table {} with pk of {}.  There was no change to apply", 
                           targetTable.getFullyQualifiedTableName(), data.getCsvData(CsvData.PK_DATA));
                }
                // There was no change to apply
                return LoadStatus.SUCCESS;
            }
        } catch (SqlException ex) {
            logFailureDetails(ex, data, true);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }
    
    @Override
    protected boolean create(CsvData data) {
        String xml = null;
        try {
            transaction.commit();

            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            xml = data.getParsedData(CsvData.ROW_DATA)[0];
            log.info("About to create table using the following definition: {}", xml);
            StringReader reader = new StringReader(xml);
            Database db = DatabaseXmlUtil.read(reader, false);
            if (writerSettings.isCreateTableAlterCaseToMatchDatabaseDefault()) {
                platform.alterCaseToMatchDatabaseDefaultCase(db);
            }
            
            platform.makePlatformSpecific(db);
           
            if (writerSettings.isAlterTable()) {
                platform.alterDatabase(db, !writerSettings.isCreateTableFailOnError());
            } else {
                platform.createDatabase(db, writerSettings.isCreateTableDropFirst(), !writerSettings.isCreateTableFailOnError());
            }

            platform.resetCachedTableModel();
            statistics.get(batch).increment(DataWriterStatisticConstants.CREATECOUNT);
            return true;
        } catch (RuntimeException ex) {
            log.error("Failed to alter table using the following xml: {}", xml);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    @Override
    protected boolean sql(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String script = data.getParsedData(CsvData.ROW_DATA)[0];
            List<String> sqlStatements = getSqlStatements(script);
            long count = 0;
            for (String sql : sqlStatements) {

                sql = preprocessSqlStatement(sql);
                transaction.prepare(sql);
                if (log.isDebugEnabled()) {
                    log.debug("About to run: {}", sql);
                }
                count += transaction.prepareAndExecute(sql);
                if (log.isDebugEnabled()) {
                    log.debug("{} rows updated when running: {}", count, sql);
                }
            }
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLCOUNT);
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLROWSAFFECTEDCOUNT,
                    count);
            return true;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }
    
    protected boolean requireNewStatement(DmlType currentType, CsvData data,
            boolean applyChangesOnly, boolean useConflictDetection,
            Conflict.DetectConflict detectType) {
        boolean requiresNew = currentDmlStatement == null || lastData == null
                || currentDmlStatement.getDmlType() != currentType
                || lastData.getDataEventType() != data.getDataEventType()
                || lastApplyChangesOnly != applyChangesOnly
                || lastUseConflictDetection != useConflictDetection;
        if (!requiresNew && currentType == DmlType.UPDATE) {
            String currentChanges = Arrays.toString(data.getChangedDataIndicators());
            String lastChanges = Arrays.toString(lastData.getChangedDataIndicators());
            requiresNew = !currentChanges.equals(lastChanges);
        }

        if (!requiresNew) {
            requiresNew |= containsNullLookupKeyDataSinceLastStatement(currentType, data,
                    detectType);
        }

        return requiresNew;
    }

    protected boolean containsNullLookupKeyDataSinceLastStatement(DmlType currentType,
            CsvData data, DetectConflict detectType) {
        boolean foundNullValueChange = false;
        if (currentType == DmlType.UPDATE || currentType == DmlType.DELETE) {
            if (detectType != null
                    && (detectType == DetectConflict.USE_CHANGED_DATA || detectType == DetectConflict.USE_OLD_DATA)) {
                String[] lastOldData = lastData.getParsedData(CsvData.OLD_DATA);
                String[] newOldData = data.getParsedData(CsvData.OLD_DATA);
                if (lastOldData != null && newOldData != null) {
                    for (int i = 0; i < lastOldData.length && i < newOldData.length; i++) {
                        String lastValue = lastOldData[i];
                        String value = newOldData[i];
                        if ((lastValue != null && value == null)
                                || (value != null && lastValue == null)) {
                            foundNullValueChange = true;
                        }
                    }
                }
            } else {
                String[] lastpkData = lastData.getParsedData(CsvData.PK_DATA);
                String[] newpkData = data.getParsedData(CsvData.PK_DATA);
                if (lastpkData != null && newpkData != null) {
                    for (int i = 0; i < lastpkData.length && i < newpkData.length; i++) {
                        String lastValue = lastpkData[i];
                        String value = newpkData[i];
                        if ((lastValue != null && value == null)
                                || (value != null && lastValue == null)) {
                            foundNullValueChange = true;
                        }
                    }
                }
            }
        }
        return foundNullValueChange;
    }
    
    @Override
    protected void targetTableWasChangedByFilter(Table oldTargetTable) {
        // allow for auto increment columns to be inserted into if appropriate
        if (oldTargetTable!=null) {
            allowInsertIntoAutoIncrementColumns(false, oldTargetTable);            
        }
        allowInsertIntoAutoIncrementColumns(true, targetTable);
    }

    private void removeExcludedColumns(Conflict conflict,
            ArrayList<Column> lookupColumns) {
        String excludedString = conflict.getDetectExpressionValue(
                DetectExpressionKey.EXCLUDED_COLUMN_NAMES);
        if (!StringUtils.isBlank(excludedString)) {
            String excludedColumns[] = excludedString.split(",");
            if (excludedColumns.length > 0) {
                Iterator<Column> iter = lookupColumns.iterator();
                while (iter.hasNext()) {
                    Column column = iter.next();
                    for (String excludedColumn : excludedColumns) {
                        if (excludedColumn.trim().equalsIgnoreCase(column.getName())) {
                            iter.remove();
                            break;
                        }
                    }
                }
            }
        }
    }

    @Override
    protected void logFailureDetails(Throwable e, CsvData data, boolean logLastDmlDetails) {
        StringBuilder failureMessage = new StringBuilder();
        failureMessage.append("Failed to process a ");
        failureMessage.append(data.getDataEventType().toString().toLowerCase());
        failureMessage.append(" event in batch ");
        failureMessage.append(batch.getBatchId());
        failureMessage.append(".\n");
        
        if (logLastDmlDetails && this.currentDmlStatement != null) {
            failureMessage.append("Failed sql was: ");
            failureMessage.append(this.currentDmlStatement.getSql());
            failureMessage.append("\n");
        }
        
        if (logLastDmlDetails && this.currentDmlValues != null) {
            failureMessage.append("Failed sql parameters: ");
            failureMessage.append(StringUtils.abbreviate(Arrays.toString(currentDmlValues), CsvData.MAX_DATA_SIZE_TO_PRINT_TO_LOG));
            failureMessage.append("\n");
            failureMessage.append("Failed sql parameters types: ");
            failureMessage.append(Arrays.toString(this.currentDmlStatement.getTypes()));
            failureMessage.append("\n");
        }

        data.writeCsvDataDetails(failureMessage);

        String msg = failureMessage.toString();
        logSuppressor.logInfo(msg, msg, null);

    }
    
    @Override
    protected void bindVariables(Map<String, Object> variables) {
        super.bindVariables(variables);
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
    }
    
    protected List<String> getSqlStatements(String script) {
        List<String> sqlStatements = new ArrayList<String>();
        SqlScriptReader scriptReader = new SqlScriptReader(new StringReader(script));
        try {
            String sql = scriptReader.readSqlStatement();
            while (sql != null) {
                sqlStatements.add(sql);
                sql = scriptReader.readSqlStatement();
            }
            return sqlStatements;
        } finally {
            IOUtils.closeQuietly(scriptReader);
        }
    }

    protected String preprocessSqlStatement(String sql) {
		sql = FormatUtils.replace("nodeId", batch.getTargetNodeId(), sql);
		if (targetTable != null) {
			sql = FormatUtils.replace("catalogName", quoteString(targetTable.getCatalog()),sql);
			sql = FormatUtils.replace("schemaName", quoteString(targetTable.getSchema()), sql);
			sql = FormatUtils.replace("tableName", quoteString(targetTable.getName()), sql);
		} else if (sourceTable != null){
			sql = FormatUtils.replace("catalogName", quoteString(sourceTable.getCatalog()),sql);
			sql = FormatUtils.replace("schemaName", quoteString(sourceTable.getSchema()), sql);
			sql = FormatUtils.replace("tableName", quoteString(sourceTable.getName()), sql);
		}

		sql = platform.scrubSql(sql);
		
        sql = FormatUtils.replace("sourceNodeId", (String) context.get("sourceNodeId"), sql);
        sql = FormatUtils.replace("sourceNodeExternalId",
                (String) context.get("sourceNodeExternalId"), sql);
        sql = FormatUtils.replace("sourceNodeGroupId", (String) context.get("sourceNodeGroupId"),
                sql);
        sql = FormatUtils.replace("targetNodeId", (String) context.get("targetNodeId"), sql);
        sql = FormatUtils.replace("targetNodeExternalId",
                (String) context.get("targetNodeExternalId"), sql);
        sql = FormatUtils.replace("targetNodeGroupId", (String) context.get("targetNodeGroupId"),
                sql);

		return sql;
    }

    protected String quoteString(String string) {
    	if (!StringUtils.isEmpty(string)) {
			String quote = platform.getDdlBuilder().isDelimitedIdentifierModeOn() ? platform
					.getDatabaseInfo().getDelimiterToken() : "";
			return String.format("%s%s%s", quote, string, quote);
    	} else {
    		return string;
    	}

    }

    protected boolean doesColumnNeedUpdated(int targetColumnIndex, Column column, CsvData data, String[] rowData, String[] oldData,
            boolean applyChangesOnly) {
        boolean needsUpdated = true;
        if (!platform.getDatabaseInfo().isAutoIncrementUpdateAllowed() && column.isAutoIncrement()) {
            needsUpdated = false;
        } else if (oldData != null && applyChangesOnly) {
            /*
             * Old data isn't captured for some lob fields. When both values are
             * null, then we always have to update because we don't know if the
             * lob field was previously null.
             */
            boolean containsEmptyLobColumn = platform.isLob(column.getMappedTypeCode())
                    && StringUtils.isBlank(oldData[targetColumnIndex]);
            needsUpdated = !StringUtils.equals(rowData[targetColumnIndex], oldData[targetColumnIndex])
                    || data.getParsedData(CsvData.OLD_DATA) == null
                    || containsEmptyLobColumn;
            if (containsEmptyLobColumn) {
                // indicate that we are considering the column to be changed
                data.getChangedDataIndicators()[sourceTable.getColumnIndex(column.getName())] = true;
            }
        } else {
            /*
             * This is in support of creating update statements that don't use
             * the keys in the set portion of the update statement. </p> In
             * oracle (and maybe not only in oracle) if there is no index on
             * child table on FK column and update is performing on PK on master
             * table, table lock is acquired on child table. Table lock is taken
             * not in exclusive mode, but lock contentions is possible.
             */
            needsUpdated = !column.isPrimaryKey()
                    || !StringUtils.equals(rowData[targetColumnIndex], getPkDataFor(data, column));
            /*
             * A primary key change isn't indicated in the change data indicators when there is no old
             * data.  Need to update it manually in that case.
             */
            data.getChangedDataIndicators()[sourceTable.getColumnIndex(column.getName())] = needsUpdated;
        }
        return needsUpdated;
    }

    protected int execute(CsvData data, String[] values) {
        currentDmlValues = platform.getObjectValues(batch.getBinaryEncoding(), values,
                currentDmlStatement.getMetaData(), false, writerSettings.isFitToColumn());
        if (log.isDebugEnabled()) {
            log.debug("Submitting data {} with types {}", Arrays.toString(currentDmlValues),
                    Arrays.toString(this.currentDmlStatement.getTypes()));
        }
        return transaction.addRow(data, currentDmlValues, this.currentDmlStatement.getTypes());
    }

    @Override
    protected Table lookupTableAtTarget(Table sourceTable) {
        String tableNameKey = sourceTable.getTableKey();
        Table table = targetTables.get(tableNameKey);
        if (table == null) {
            table = platform.getTableFromCache(sourceTable.getCatalog(), sourceTable.getSchema(),
                    sourceTable.getName(), false);
            if (table != null) {
                table = table.copyAndFilterColumns(sourceTable.getColumnNames(),
                        sourceTable.getPrimaryKeyColumnNames(),
                        this.writerSettings.isUsePrimaryKeysFromSource());

                Column[] columns = table.getColumns();
                for (Column column : columns) {
                    if (column != null) {
                        int typeCode = column.getMappedTypeCode();
                        if (this.writerSettings.isTreatDateTimeFieldsAsVarchar()
                                && (typeCode == Types.DATE || typeCode == Types.TIME || typeCode == Types.TIMESTAMP)) {
                            column.setMappedTypeCode(Types.VARCHAR);
                        }
                    }
                }

                targetTables.put(tableNameKey, table);
            }
        }
        return table;
    }

    public DmlStatement getCurrentDmlStatement() {
        return currentDmlStatement;
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

    protected String getCurData(ISqlTransaction transaction) {
        String curVal = null;
        if (writerSettings.isSaveCurrentValueOnError()) {
            String[] keyNames = Table.getArrayColumns(context.getTable().getPrimaryKeyColumns());
            String[] columnNames = Table.getArrayColumns(context.getTable().getColumns());

            org.jumpmind.db.model.Table targetTable = platform.getTableFromCache(
                    context.getTable().getCatalog(), context.getTable().getSchema(),
                    context.getTable().getName(), false);

            targetTable = targetTable.copyAndFilterColumns(columnNames, keyNames, true);

            String[] data = context.getData().getParsedData(CsvData.OLD_DATA);
            if (data == null) {
                data = context.getData().getParsedData(CsvData.ROW_DATA);
            }

            Column[] columns = targetTable.getColumns();

            Object[] objectValues = platform.getObjectValues(context.getBatch().getBinaryEncoding(), data,
                    columns);

            Map<String, Object> columnDataMap = CollectionUtils
                    .toMap(columnNames, objectValues);

            Column[] pkColumns = targetTable.getPrimaryKeyColumns();
            Object[] args = new Object[pkColumns.length];
            for (int i = 0; i < pkColumns.length; i++) {
                args[i] = columnDataMap.get(pkColumns[i].getName());
            }

            DmlStatement sqlStatement = platform
                    .createDmlStatement(DmlType.SELECT, targetTable, writerSettings.getTextColumnExpression());


            Row row = null;
            List<Row> list =  transaction.query(sqlStatement.getSql(),
                    new ISqlRowMapper<Row>() {
                        public Row mapRow(Row row) {
                            return row;
                        }
            }
                    , args, null);

            if (list != null && list.size() > 0) {
                row=list.get(0);
            }

            if (row != null) {
                String[] existData = platform.getStringValues(context.getBatch().getBinaryEncoding(),
                        columns, row, false, false);
                if (existData != null) {
                    curVal =  CsvUtils.escapeCsvData(existData);
                }
            }
        }
        return curVal;

    }
    
    @Override
    protected void allowInsertIntoAutoIncrementColumns(boolean value, Table table) {
        DatabaseInfo dbInfo = platform.getDatabaseInfo();
        String quote = dbInfo.getDelimiterToken();
        String catalogSeparator = dbInfo.getCatalogSeparator();
        String schemaSeparator = dbInfo.getSchemaSeparator();
        transaction.allowInsertIntoAutoIncrementColumns(value, table, quote, catalogSeparator, schemaSeparator);
    }
    
}
