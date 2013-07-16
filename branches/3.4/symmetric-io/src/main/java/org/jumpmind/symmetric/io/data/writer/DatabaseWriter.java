/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.io.DatabaseXmlUtil;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.DmlStatement;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.CsvData;
import org.jumpmind.symmetric.io.data.CsvUtils;
import org.jumpmind.symmetric.io.data.DataContext;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.io.data.IDataWriter;
import org.jumpmind.symmetric.io.data.writer.Conflict.DetectConflict;
import org.jumpmind.util.CollectionUtils;
import org.jumpmind.util.FormatUtils;
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
    
    public static final String CUR_DATA="DatabaseWriter.CurData";
    
    protected IDatabasePlatform platform;

    protected ISqlTransaction transaction;

    protected DmlStatement currentDmlStatement;

    protected boolean lastUseConflictDetection = true;

    protected boolean lastApplyChangesOnly = false;

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

    protected Set<String> missingTables = new HashSet<String>();

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
        if (table == null) {
            throw new NullPointerException("Cannot load a null table");
        }
        this.lastData = null;
        this.currentDmlStatement = null;
        this.sourceTable = table;
        this.targetTable = lookupTableAtTarget(table);
        if (this.targetTable != null || hasFilterThatHandlesMissingTable(table)) {
            String quote = getPlatform().getDatabaseInfo().getDelimiterToken();
            this.transaction.allowInsertIntoAutoIncrementColumns(true, this.targetTable, quote);
            return true;
        } else if (writerSettings.isIgnoreMissingTables()) {
            String qualifiedName = sourceTable.getFullyQualifiedTableName();
            if (!missingTables.contains(qualifiedName)) {
                log.warn("Did not find the {} table in the target database", qualifiedName);
                missingTables.add(qualifiedName);
            }
            return false;
        } else {
            // The first data should fail because the table will not be found
            return true;
        }
    }

    public void write(CsvData data) {
    	write(data, false);
    }
    
    protected void write(CsvData data, boolean fallback) {
        if (data.requiresTable() && 
        		(targetTable == null && data.getDataEventType() != DataEventType.SQL)) {
            // if we cross batches and the table isn't specified, then
            // use the last table we used
            start(context.getLastParsedTable());
        }
        if (targetTable != null || !data.requiresTable() || 
        		(targetTable == null && data.getDataEventType() == DataEventType.SQL)) {
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
                        if (conflictResolver != null && !fallback) {
                            conflictResolver.needsResolved(this, data, loadStatus);
                        } else {
                            throw new ConflictException(data, targetTable, false, writerSettings.pickConflict(targetTable, batch));
                        }
                    } else {
                        uncommittedCount++;
                    }

                    lastData = data;

                    filterAfter(data);

                    checkForEarlyCommit();

                }

            } catch (IgnoreBatchException ex) {
                rollback();
                throw ex;
            } catch (RuntimeException ex) {
                if (filterError(data, ex)) {
                    throw ex;
                } else {
                    uncommittedCount++;
                    statistics.get(batch).increment(DataWriterStatisticConstants.IGNORECOUNT);
                    checkForEarlyCommit();
                }
            }
        } else {
            if (sourceTable != null) {
                throw new SqlException(String.format("Could not find the target table %s",
                        sourceTable.getFullyQualifiedTableName()));
            } else {
                throw new SqlException("The target table was not specified");
            }
        }
    }
    
    protected void checkForEarlyCommit() {
        if (uncommittedCount >= writerSettings.getMaxRowsBeforeCommit()) {
            commit(true);
            
            long sleep = writerSettings.getCommitSleepInterval();
            if (sleep > 0) {
                /*
                 * Chances are if SymmetricDS is configured to commit early in a
                 * batch we want to give other threads a chance to do work and
                 * access the database. This was added to support H2 clients
                 * that are loading big batches while an application is doing
                 * work.
                 */
                try {
                    Thread.sleep(sleep);
                } catch (InterruptedException e) {
                    log.warn("{}", e.getMessage());
                }
            }
        }
    }

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

    protected boolean filterError(CsvData data, Exception ex) {
        boolean process = true;
        List<IDatabaseWriterErrorHandler> filters = this.writerSettings
                .getDatabaseWriterErrorHandlers();
        if (filters != null) {
            try {
                statistics.get(batch).startTimer(DataWriterStatisticConstants.FILTERMILLIS);
                for (IDatabaseWriterErrorHandler filter : filters) {
                    process &= filter.handleError(context, targetTable, data, ex);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
        return process;
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
            if (requireNewStatement(DmlType.INSERT, data, false, true, null)) {
                this.lastUseConflictDetection = true;                
                this.currentDmlStatement = platform.createDmlStatement(DmlType.INSERT, targetTable);
                if (log.isDebugEnabled()) {
                    log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                }
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                String[] values = (String[]) ArrayUtils.addAll(getRowData(data, CsvData.ROW_DATA),
                        getLookupKeyData(getLookupDataMap(data), this.currentDmlStatement));
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
                        context.put(CUR_DATA,getCurData(transaction));
                        return LoadStatus.CONFLICT;
                    } else {
                        log.warn("Detected a conflict via an exception, but cannot perform conflict resolution because the database in use requires savepoints");
                        throw ex;
                    }
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

    protected String[] getRowData(CsvData data, String dataType) {
        String[] targetValues = new String[targetTable.getColumnCount()];
        String[] targetColumnNames = targetTable.getColumnNames();

        String[] originalValues = data.getParsedData(dataType);
        String[] sourceColumnNames = sourceTable.getColumnNames();

        if (originalValues != null) {
            for (int i = 0; i < sourceColumnNames.length && i < originalValues.length; i++) {
                for (int t = 0; t < targetColumnNames.length; t++) {
                    if (sourceColumnNames[i].equalsIgnoreCase(targetColumnNames[t])) {
                        targetValues[t] = originalValues[i];
                        break;
                    }
                }
            }
        }
        return targetValues;
    }
    
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

                if (!platform.getDatabaseInfo().isBlobsWorkInWhereClause()
                        || data.isNoBinaryOldData()) {
                    Iterator<Column> it = lookupKeys.iterator();
                    while (it.hasNext()) {
                        Column col = it.next();
                        if (col.isOfBinaryType()) {
                            it.remove();
                        }
                    }
                }

                lookupDataMap = getLookupDataMap(data);

                boolean[] nullKeyValues = new boolean[lookupKeys.size()];
                for (int i = 0; i < lookupKeys.size(); i++) {
                    Column column = lookupKeys.get(i);
                    nullKeyValues[i] = !column.isRequired()
                            && lookupDataMap.get(column.getName()) == null;
                }

                this.currentDmlStatement = platform.createDmlStatement(DmlType.DELETE,
                        targetTable.getCatalog(), targetTable.getSchema(), targetTable.getName(),
                        lookupKeys.toArray(new Column[lookupKeys.size()]), null, nullKeyValues);
                if (log.isDebugEnabled()) {
                    log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                }
                transaction.prepare(this.currentDmlStatement.getSql());
            }
            try {
                lookupDataMap = lookupDataMap == null ? getLookupDataMap(data) : lookupDataMap;
                long count = execute(data, getLookupKeyData(lookupDataMap, currentDmlStatement));
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
            logFailure(ex, data);
            throw ex;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }

    }

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

                    if (!platform.getDatabaseInfo().isBlobsWorkInWhereClause() 
                            || data.isNoBinaryOldData()) {
                        Iterator<Column> it = lookupKeys.iterator();
                        while (it.hasNext()) {
                            Column col = it.next();
                            if (col.isOfBinaryType()) {
                                it.remove();
                            }
                        }
                    }

                    lookupDataMap = getLookupDataMap(data);

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
                            nullKeyValues);
                    if (log.isDebugEnabled()) {
                        log.debug("Preparing dml: " + this.currentDmlStatement.getSql());
                    }
                    transaction.prepare(this.currentDmlStatement.getSql());

                }

                rowData = (String[]) changedColumnValueList
                        .toArray(new String[changedColumnValueList.size()]);
                lookupDataMap = lookupDataMap == null ? getLookupDataMap(data) : lookupDataMap;
                String[] values = (String[]) ArrayUtils.addAll(rowData,
                        getLookupKeyData(lookupDataMap, currentDmlStatement));

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

        final long MAX_DATA_SIZE_TO_PRINT_TO_LOG = 1024 * 1000;
        rowData = data.getCsvData(CsvData.ROW_DATA);
        if (StringUtils.isNotBlank(rowData)) {
            if (rowData.length() < MAX_DATA_SIZE_TO_PRINT_TO_LOG) {
                failureMessage.append("Failed row data was: ");
                failureMessage.append(rowData);
                failureMessage.append("\n");
            } else {
                failureMessage.append("Row data was bigger than ");
                failureMessage.append(MAX_DATA_SIZE_TO_PRINT_TO_LOG);
                failureMessage.append(" bytes (it was ");
                failureMessage.append(rowData.length());
                failureMessage.append(" bytes).  It will not be printed to the log file");
            }
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
            String script = data.getParsedData(CsvData.ROW_DATA)[0];
            Map<String, Object> variables = new HashMap<String, Object>();
            variables.put("SOURCE_NODE_ID", batch.getSourceNodeId());
            variables.put("TARGET_NODE_ID", batch.getTargetNodeId());
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

            if (log.isDebugEnabled()) {
                log.debug("About to run: {}", script);
            }
            interpreter.eval(script);
            statistics.get(batch).increment(DataWriterStatisticConstants.SCRIPTCOUNT);
        } catch (EvalError e) {
            throw new RuntimeException(e);
        }
        return true;
    }

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

    protected boolean sql(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String sql = preprocessSqlStatement(data);
            transaction.prepare(sql);
            if (log.isDebugEnabled()) {
                log.debug("About to run: {}", sql);
            }
            long count = transaction.prepareAndExecute(sql);
            if (log.isDebugEnabled()) {
                log.debug("{} rows updated when running: {}", count, sql);
            }
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLCOUNT);
            statistics.get(batch).increment(DataWriterStatisticConstants.SQLROWSAFFECTEDCOUNT,
                    count);
            return true;
        } finally {
            statistics.get(batch).stopTimer(DataWriterStatisticConstants.DATABASEMILLIS);
        }
    }

    protected String preprocessSqlStatement(CsvData data) {

		String sql = data.getParsedData(CsvData.ROW_DATA)[0];
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
		
//		sql = FormatUtils.replace("groupId", node.getNodeGroupId(), sql);
//		sql = FormatUtils.replace("externalId", node.getExternalId(), sql);

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
    
	protected boolean doesColumnNeedUpdated(int columnIndex, Column column,
			CsvData data, String[] rowData, String[] oldData,
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
                    && StringUtils.isBlank(oldData[columnIndex]);
            needsUpdated = !StringUtils.equals(rowData[columnIndex], oldData[columnIndex])
                    || containsEmptyLobColumn;
            if (containsEmptyLobColumn) {
                // indicate that we are considering the column to be changed
                Column sourceColumn = sourceTable.findColumn(column.getName(), false);
                data.getChangedDataIndicators()[sourceTable.getColumnIndex(sourceColumn.getName())] = true;                
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
                    || !StringUtils.equals(rowData[columnIndex], getPkDataFor(data, column));
        }
        return needsUpdated;
    }

    protected Map<String, String> getLookupDataMap(CsvData data) {
        Map<String, String> keyData = null;
        if (data.getDataEventType() == DataEventType.INSERT) {
            keyData = data.toColumnNameValuePairs(sourceTable.getColumnNames(), CsvData.ROW_DATA);
        } else {
            keyData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                    CsvData.OLD_DATA);
            if (keyData == null || keyData.size() == 0) {
                keyData = data.toColumnNameValuePairs(sourceTable.getPrimaryKeyColumnNames(),
                        CsvData.PK_DATA);
            }
            if (keyData == null || keyData.size() == 0) {
                keyData = data.toColumnNameValuePairs(sourceTable.getColumnNames(),
                        CsvData.ROW_DATA);
            }            
        }
        return keyData;
    }

    protected String[] getLookupKeyData(Map<String, String> lookupDataMap, DmlStatement dmlStatement) {
        Column[] lookupColumns = dmlStatement.getKeys();
        if (lookupColumns != null && lookupColumns.length > 0) {
            if (lookupDataMap != null && lookupDataMap.size() > 0) {
                String[] keyDataAsArray = new String[lookupColumns.length];
                int index = 0;
                for (Column keyColumn : lookupColumns) {
                    keyDataAsArray[index++] = lookupDataMap.get(keyColumn.getName());
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
        if (log.isDebugEnabled()) {
            log.debug("Submitting data {} with types {}", Arrays.toString(objectValues),
                    Arrays.toString(this.currentDmlStatement.getTypes()));
        }
        return transaction.addRow(data, objectValues, this.currentDmlStatement.getTypes());
    }

    public void end(Table table) {
        String quote = getPlatform().getDatabaseInfo().getDelimiterToken();
        this.transaction.allowInsertIntoAutoIncrementColumns(false, this.targetTable, quote);
    }

    public void end(Batch batch, boolean inError) {
        this.lastData = null;
        this.currentDmlStatement = null;
        if (batch.isIgnored()) {
            getStatistics().get(batch).increment(DataWriterStatisticConstants.IGNORECOUNT);
        }
        if (!inError) {
            notifyFiltersBatchComplete();
            commit(false);
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
                table = table.copyAndFilterColumns(sourceTable.getColumnNames(),
                        sourceTable.getPrimaryKeyColumnNames(),
                        this.writerSettings.isUsePrimaryKeysFromSource());

                if (StringUtils.isBlank(sourceTable.getCatalog())) {
                    table.setCatalog(null);
                }

                if (StringUtils.isBlank(sourceTable.getSchema())) {
                    table.setSchema(null);
                }

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
    
    public Table getSourceTable() {
        return sourceTable;
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
                    .createDmlStatement(DmlType.SELECT, targetTable);
            
            
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
                        columns, row, false);
                if (existData != null) {
                    curVal =  CsvUtils.escapeCsvData(existData);
                }
            }
        }
        return curVal;

    }
}
