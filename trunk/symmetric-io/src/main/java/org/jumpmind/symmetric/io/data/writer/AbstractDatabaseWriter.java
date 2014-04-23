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

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
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

abstract public class AbstractDatabaseWriter implements IDataWriter {

    protected final static Logger log = LoggerFactory.getLogger(AbstractDatabaseWriter.class);

    public static enum LoadStatus {
        SUCCESS, CONFLICT
    };

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

    public AbstractDatabaseWriter() {
        this(null, null);
    }

    public AbstractDatabaseWriter(DatabaseWriterSettings settings) {
        this(null, settings);
    }

    public AbstractDatabaseWriter(
            IDatabaseWriterConflictResolver conflictResolver, DatabaseWriterSettings settings) {
        this.conflictResolver = conflictResolver == null ? new DefaultDatabaseWriterConflictResolver()
                : conflictResolver;
        this.writerSettings = settings == null ? new DatabaseWriterSettings() : settings;
    }

    public void open(DataContext context) {
        this.context = context;
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
        this.sourceTable = table;
        this.targetTable = lookupTableAtTarget(this.sourceTable);
        this.sourceTable.copyColumnTypesFrom(this.targetTable);
        if (this.targetTable==null && hasFilterThatHandlesMissingTable(table)) {
            this.targetTable = table;
        }
        if (this.targetTable != null) {
            return true;
        } else if (writerSettings.isIgnoreMissingTables() || sourceTable.getName().toLowerCase().endsWith("console_user")) {
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
                    }
                    
                    uncommittedCount++;

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
                // If the source table was found but the target table is still unknown throw an exception
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
        uncommittedCount = 0;
    }
    
    protected void rollback() {
        uncommittedCount = 0;
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
                    process &= filter.beforeWrite(this.context, this.sourceTable, data);
                }
                // re-lookup target table in case the source table has changed
                Table oldTargetTable = targetTable;
                if (this.sourceTable!=null) {
                    targetTable = lookupTableAtTarget(this.sourceTable);
                }
                if (targetTable!=null && !targetTable.equals(oldTargetTable)) {
                     targetTableWasChangedByFilter(oldTargetTable);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
        return process;
    }
    
    protected void targetTableWasChangedByFilter(Table oldTargetTable) {
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
                    filter.afterWrite(this.context, this.sourceTable, data);
                }
            } finally {
                statistics.get(batch).stopTimer(DataWriterStatisticConstants.FILTERMILLIS);
            }
        }
    }

    protected abstract LoadStatus insert(CsvData data);
    protected abstract LoadStatus delete(CsvData data, boolean useConflictDetection);
    protected abstract LoadStatus update(CsvData data, boolean applyChangesOnly, boolean useConflictDetection);
    protected abstract boolean create(CsvData data);
    protected abstract boolean sql(CsvData data);
    
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
    
    protected void bindVariables(Map<String, Object> variables) {
        variables.put("SOURCE_NODE_ID", batch.getSourceNodeId());
        variables.put("TARGET_NODE_ID", batch.getTargetNodeId());
        variables.putAll(context.getContext());
    }

    protected boolean script(CsvData data) {
        try {
            statistics.get(batch).startTimer(DataWriterStatisticConstants.DATABASEMILLIS);
            String script = data.getParsedData(CsvData.ROW_DATA)[0];
            Map<String, Object> variables = new HashMap<String, Object>();
            bindVariables(variables);
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

    public void end(Table table) {
    }

    public void end(Batch batch, boolean inError) {
        this.lastData = null;
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
    
    protected void allowInsertIntoAutoIncrementColumns(boolean value, Table table) {
        
    }

    protected Table lookupTableAtTarget(Table sourceTable) {
       return sourceTable;
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

    public Table getTargetTable() {
        return targetTable;
    }

    public Table getSourceTable() {
        return sourceTable;
    }

    public Map<Batch, Statistics> getStatistics() {
        return statistics;
    }

    public DatabaseWriterSettings getWriterSettings() {
        return writerSettings;
    }
    

}
