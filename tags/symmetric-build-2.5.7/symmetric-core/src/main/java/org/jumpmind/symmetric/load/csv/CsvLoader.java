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
 * under the License.  */
package org.jumpmind.symmetric.load.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.csv.CsvReader;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.DataLoaderStatistics;
import org.jumpmind.symmetric.load.IBatchListener;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.load.IMissingTableHandler;
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SingleConnectionDataSource;

public class CsvLoader implements IDataLoader {

    static final ILog log = LogFactory.getLog(CsvLoader.class);

    protected JdbcTemplate jdbcTemplate;

    protected IDbDialect dbDialect;

    protected IParameterService parameterService;

    protected INodeService nodeService;

    protected CsvReader csvReader;

    protected DataLoaderContext context;

    protected DataLoaderStatistics stats;

    protected List<IDataLoaderFilter> filters;
    
    protected IStatisticManager statisticManager;

    protected Map<String,  List<IColumnFilter>> columnFilters;
    
    protected long bytesCount = 0;
    
    protected long lineCount = 0;
    
    protected Connection connection;
    
    protected boolean oldAutoCommitSetting;
    
    protected List<IBatchListener> batchListeners;
    
    private static Set<String> missingTables = new HashSet<String>();

    public void open(BufferedReader reader, DataSource dataSource, List<IBatchListener> batchListeners) throws IOException {
        try {
            this.connection = dataSource.getConnection();
            this.oldAutoCommitSetting = this.connection.getAutoCommit();
            this.connection.setAutoCommit(false);
            this.batchListeners = batchListeners != null ? batchListeners : new ArrayList<IBatchListener>();
            this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            this.jdbcTemplate.setQueryTimeout(dbDialect.getQueryTimeoutInSeconds());
            this.csvReader = CsvUtils.getCsvReader(reader);
            this.context = new DataLoaderContext(nodeService, jdbcTemplate);
            this.stats = new DataLoaderStatistics();
        } catch (SQLException ex) {
            close();
            throw new RuntimeException(ex);
        }
    }

    public void open(BufferedReader reader, DataSource dataSource, List<IBatchListener> batchListeners, List<IDataLoaderFilter> filters, Map<String,  List<IColumnFilter>> columnFilters)
            throws IOException {
        open(reader, dataSource, batchListeners);
        this.filters = filters;
        this.columnFilters = columnFilters;
    }

    public boolean hasNext() throws IOException {
        context.clearBatch();
        while (csvReader.readRecord()) {
            String[] tokens = csvReader.getValues();
            if (tokens[0].equals(CsvConstants.BATCH)) {
                this.context.setBatchId(new Long(tokens[1]));
                this.stats = new DataLoaderStatistics();
                return true;
            } else if (tokens[0].equals(CsvConstants.NODEID)) {
                this.context.setSourceNodeId(tokens[1]);
            } else {
                if (!isMetaTokenParsed(tokens)) {
                    log.debug("LoaderIgnoringToken", tokens[0]);
                }
            }
        }
        return false;
    }

    public void skip() throws IOException {
        context.setSkipping(true);
        load();
        // skipping is reset when a new batch_id is set
    }
    
    private void logTableIgnored() {
        TableTemplate tableTemplate = context.getTableTemplate();
        if (tableTemplate != null) {
            String tableName = tableTemplate.getFullyQualifiedTableName();
            if (!missingTables.contains(tableName)) {
                log.warn("LoaderTableMissing", tableName);
                missingTables.add(tableName);
            }
        }
    }

    public void load() throws IOException {
        try {
            dbDialect.disableSyncTriggers(jdbcTemplate, context.getSourceNodeId());
            long rowsBeforeCommit = parameterService.getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT);
            long rowsProcessed = 0;
            lineCount = 0;
            bytesCount = 0;
            long ts = System.currentTimeMillis();
            prepareTableForDataLoad();
            while (csvReader.readRecord()) {
                TableTemplate tableTemplate = context.getTableTemplate();
                String[] tokens = csvReader.getValues();
                if (tokens != null && tokens.length > 0 && tokens[0] != null) {
                    stats.incrementLineCount();
                    lineCount++;
                    long numberOfBytes = csvReader.getRawRecord().length();
                    stats.incrementByteCount(numberOfBytes);
                    bytesCount += numberOfBytes;
                    if (tokens[0].equals(CsvConstants.INSERT)) {
                        if (tableTemplate == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (tableTemplate.isIgnoreThisTable() && !willFiltersHandleMissingTable(context)) {
                            logTableIgnored();
                        } else if (!context.isSkipping()) {
                            insert(tokens);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.UPDATE)) {
                        if (tableTemplate == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (tableTemplate.isIgnoreThisTable() && !willFiltersHandleMissingTable(context)) {
                            logTableIgnored();
                        } else if (!context.isSkipping()) {
                            update(tokens);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.DELETE)) {
                        if (tableTemplate == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (tableTemplate.isIgnoreThisTable() && !willFiltersHandleMissingTable(context)) {
                            logTableIgnored();
                        } else if (!context.isSkipping()) {
                            delete(tokens);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.OLD)) {
                        context.setOldData((String[]) ArrayUtils.subarray(tokens, 1, tokens.length));
                    } else if (isMetaTokenParsed(tokens)) {
                        continue;
                    } else if (tokens[0].equals(CsvConstants.COMMIT)) {
                        break;
                    } else if (tokens[0].equals(CsvConstants.SQL)) {
                        if (tableTemplate == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (tableTemplate.isIgnoreThisTable()) {
                            logTableIgnored();
                        } else if (!context.isSkipping()) {
                            runSql(tokens[1]);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.BSH)) {
                        if (!context.isSkipping()) {
                            Map<String, Object> variables = new HashMap<String, Object>();
                            Node identity = nodeService.findIdentity();
                            variables.put("SOURCE_NODE_ID", context.getNodeId());
                            variables.put("DATASOURCE", dbDialect.getJdbcTemplate().getDataSource());
                            if (identity != null) {
                                variables.put("TARGET_NODE_ID", identity.getNodeId());
                                variables.put("TARGET_EXTERNAL_ID", identity.getExternalId());
                                variables.put("TARGET_NODE", identity);
                            }
                            AppUtils.runBsh(variables, tokens[1]);
                            rowsProcessed++;
                        }                        
                    } else if (tokens[0].equals(CsvConstants.CREATE)) {
                        if (!context.isSkipping()) {
                            runDdl(tokens[1]);
                            rowsProcessed++;
                        }
                    } else {
                        log.warn("LoaderTokenUnexpected", tokens[0], stats.getLineCount(), context.getBatch());
                    }
                }
                
                if (rowsProcessed > rowsBeforeCommit && rowsBeforeCommit > 0) {
                    rowsProcessed = 0;       
                    fireEarlyCommit();
                    connection.commit();
                    // Chances are if SymmetricDS is configured to commit early in a batch we want to give other threads
                    // a chance to do work and access the database.
                    AppUtils.sleep(5);                    
                }
                
                if (bytesCount > StatisticConstants.FLUSH_SIZE_BYTES) {
                    statisticManager.incrementDataBytesLoaded(context.getChannelId(), bytesCount);
                    bytesCount = 0;
                }
                
                if (lineCount > StatisticConstants.FLUSH_SIZE_LINES) {
                    statisticManager.incrementDataLoaded(context.getChannelId(), lineCount);
                    lineCount = 0;
                }
                
                long executeTimeInMs = System.currentTimeMillis()-ts;
                if (executeTimeInMs >  DateUtils.MILLIS_PER_MINUTE * 10) {
                    log.warn("LongRunningOperation", "loaded " + stats.getLineCount() + " data so far for batch " + context.getBatchId(), executeTimeInMs);
                    ts = System.currentTimeMillis();
                }

            }
            
            fireBatchComplete();
            connection.commit();
            fireBatchCommitted();
            rowsProcessed = 0;

        } catch (RuntimeException ex) {
            rollback(ex);            
            throw ex;
        } catch (Exception ex) {
            rollback(ex);
            throw new RuntimeException(ex);
        } finally {
            try {
                if (bytesCount > 0) {
                    statisticManager.incrementDataBytesLoaded(context.getChannelId(), bytesCount);
                }

                if (lineCount > 0) {
                    statisticManager.incrementDataLoaded(context.getChannelId(), lineCount);
                }
                cleanupAfterDataLoad();                
            } finally {
                try {
                    dbDialect.enableSyncTriggers(jdbcTemplate);
                } catch (Exception ex) {
                    log.error(ex);
                }
            }
        }
    }   
    
    protected boolean willFiltersHandleMissingTable(DataLoaderContext context) {
        if (filters != null) {
            for (IDataLoaderFilter filter : filters) {
                if (filter instanceof IMissingTableHandler
                        && ((IMissingTableHandler) filter).isHandlingMissingTable(context)) {
                    return true;
                }
            }
        }
        return false;
    }
    
    protected void rollback(Exception ex) {
        try {
            connection.rollback();
        } catch (Exception ex2) {
            log.warn(ex2);
        } finally {
            fireBatchRolledback(ex);
        }
    }

    protected boolean isMetaTokenParsed(String[] tokens) {
        boolean isMetaTokenParsed = true;
        if (tokens[0].equals(CsvConstants.SCHEMA)) {
            context.setSchemaName(StringUtils.isBlank(tokens[1]) ? null : tokens[1]);
        } else if (tokens[0].equals(CsvConstants.CATALOG)) {
            context.setCatalogName(StringUtils.isBlank(tokens[1]) ? null : tokens[1]);
        } else if (tokens[0].equals(CsvConstants.TABLE)) {
            context.setTableName(tokens[1]);
            resetTable();
        } else if (tokens[0].equals(CsvConstants.KEYS)) {
            context.setKeyNames((String[]) ArrayUtils.subarray(tokens, 1, tokens.length));
        } else if (tokens[0].equals(CsvConstants.COLUMNS)) {
            context.setColumnNames((String[]) ArrayUtils.subarray(tokens, 1, tokens.length));
        } else if (tokens[0].equals(CsvConstants.BINARY)) {
            context.setBinaryEncodingType(tokens[1]);
        } else if (tokens[0].equals(CsvConstants.CHANNEL)) {
            context.setChannelId(tokens[1]);
        } else {
            isMetaTokenParsed = false;
        }
        return isMetaTokenParsed;
    }

    protected void resetTable() {
        cleanupAfterDataLoad();
        context.chooseTableTemplate();
        if (context.getTableTemplate() == null) {
            context.setTableTemplate(new TableTemplate(jdbcTemplate, dbDialect, context.getTableName(),
                    this.columnFilters != null ? this.columnFilters.get(context.getTableName()) : null, parameterService
                            .is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE), context.getSchemaName(), context.getCatalogName()));
        }
        prepareTableForDataLoad();
    }

    protected void prepareTableForDataLoad() {
        if (context != null && context.getTableTemplate() != null) {
            dbDialect.allowIdentityInserts(this.jdbcTemplate, context.getTableTemplate().getTable());
        }
    }

    protected void cleanupAfterDataLoad() {
        if (context != null && context.getTableTemplate() != null) {
            dbDialect.revertAllowIdentityInserts(this.jdbcTemplate, context.getTableTemplate().getTable());
        }
    }

    protected int insert(String[] tokens) {
        stats.incrementStatementCount();
        String[] columnValues = context.getTableTemplate().parseColumns(tokens, 1);
        int rows = 0;

        boolean continueToLoad = true;
        if (filters != null) {
            stats.startTimer();
            for (IDataLoaderFilter filter : filters) {
                continueToLoad &= filter.filterInsert(context, columnValues);
            }
            stats.incrementFilterMillis(stats.endTimer());
        }

        if (continueToLoad) {
            String keyValues[] = context.getTableTemplate().parseKeys(tokens, 1);
            boolean attemptFallbackUpdate = false;
            RuntimeException insertException = null;
            try {
                stats.startTimer();
                try {
                    rows = context.getTableTemplate().insert(context, columnValues, keyValues);
                    if (rows == 0) {
                        attemptFallbackUpdate =  parameterService
                        .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE);
                        if (!attemptFallbackUpdate) {
                            throw new SymmetricException("LoaderInsertingFailed", insertException, context
                                    .getTableTemplate().getTable().toVerboseString(),
                                    ArrayUtils.toString(tokens));
                        }
                    }
                } catch (RuntimeException e) {
                    insertException = e;
                    attemptFallbackUpdate = dbDialect.isPrimaryKeyViolation(e)
                            && parameterService
                                    .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE);
                    if (!attemptFallbackUpdate) {
                        throw e;
                    }
                }

                if (attemptFallbackUpdate) {
                    if (log.isDebugEnabled()) {
                        log.debug("LoaderInsertingFailedUpdating", context.getTableName(),
                                ArrayUtils.toString(tokens));
                    }
                    stats.incrementFallbackUpdateCount();
                    rows = context.getTableTemplate().update(context, columnValues, keyValues);
                    if (rows == 0) {
                        throw new SymmetricException("LoaderFallbackUpdateFailed", insertException, context
                                .getTableTemplate().getTable().toVerboseString(),
                                ArrayUtils.toString(tokens), ArrayUtils.toString(keyValues));
                    }
                }

            } finally {
                stats.incrementDatabaseMillis(stats.endTimer());
            }
        }
        return rows;
    }

    protected int update(String[] tokens) {
        try {
            stats.incrementStatementCount();
            TableTemplate tableTemplate = context.getTableTemplate();
            String columnValues[] = tableTemplate.parseColumns(tokens, 1);
            String keyValues[] = tableTemplate.parseKeys(tokens, 1 + columnValues.length);
            int rows = 0;
            boolean continueToLoad = true;
            if (filters != null) {
                stats.startTimer();
                for (IDataLoaderFilter filter : filters) {
                    continueToLoad &= filter.filterUpdate(context, columnValues, keyValues);
                }
                stats.incrementFilterMillis(stats.endTimer());
            }

            if (continueToLoad) {
                boolean enableFallbackInsert = parameterService
                        .is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_INSERT);
                stats.startTimer();
                rows = context.getTableTemplate().update(context, columnValues, keyValues);
                if (rows == 0) {
                    if (enableFallbackInsert) {

                        log.debug("LoaderUpdatingFailedInserting", context.getTableName(),
                                ArrayUtils.toString(tokens));

                        stats.incrementFallbackInsertCount();
                        rows = context.getTableTemplate().insert(context, columnValues, keyValues);
                    } else {
                        stats.incrementDatabaseMillis(stats.endTimer());
                        throw new SymmetricException("LoaderUpdatingFailed",
                                context.getTableName(), ArrayUtils.toString(tokens));
                    }
                } else if (rows > 1) {
                    log.warn("LoaderRowsUpdatingFailed", rows, context.getTableName(),
                            ArrayUtils.toString(tokens));
                } else if (rows < 0) {
                    rows = context.getTableTemplate().delete(context, keyValues);
                    rows = context.getTableTemplate().insert(context, columnValues, keyValues);
                }
                stats.incrementDatabaseMillis(stats.endTimer());
            }
            return rows;
        } finally {
            context.getTableTemplate().setOldData(null);
        }
    }

    protected int delete(String[] tokens) {
        stats.incrementStatementCount();
        String keyValues[] = context.getTableTemplate().parseKeys(tokens, 1);
        int rows = 0;
        boolean continueToLoad = true;

        if (filters != null) {
            stats.startTimer();
            for (IDataLoaderFilter filter : filters) {
                continueToLoad &= filter.filterDelete(context, keyValues);
            }
            stats.incrementFilterMillis(stats.endTimer());
        }

        if (continueToLoad) {
            boolean allowMissingDelete = parameterService.is(ParameterConstants.DATA_LOADER_ALLOW_MISSING_DELETE);
            stats.startTimer();
            rows = context.getTableTemplate().delete(context, keyValues);
            stats.incrementDatabaseMillis(stats.endTimer());
            if (rows == 0) {
                if (allowMissingDelete) {
                    log.warn("LoaderDeleteMissing", context.getTableName(), ArrayUtils.toString(tokens));
                    stats.incrementMissingDeleteCount();
                } else {
                    throw new SymmetricException("LoaderDeleteMissing", context.getTableName(), ArrayUtils
                            .toString(tokens));
                }
            }
        }
        return rows;
    }

    protected void runSql(String sql) {
        stats.incrementStatementCount();
        log.debug("ScriptRunning", sql);
        jdbcTemplate.execute(sql);
        if (context.getTableTemplate() != null) {
            context.getTableTemplate().resetMetaData(false);
        }
    }

    protected void runDdl(String xml) {
        stats.incrementStatementCount();
        log.debug("DDLRunning", xml);
        dbDialect.createTables(xml);
        if (context.getTableTemplate() != null) {
            context.getTableTemplate().resetMetaData(false);
        }
    }

    public IDataLoader clone() {
        CsvLoader dataLoader = new CsvLoader();
        dataLoader.setDbDialect(dbDialect);
        dataLoader.setParameterService(parameterService);
        dataLoader.setNodeService(nodeService);
        dataLoader.setStatisticManager(statisticManager);
        return dataLoader;
    }

    public void close() {
        try {
            if (csvReader != null) {
                csvReader.close();
            }
        } finally {
            if (connection != null) {
                try {
                    connection.setAutoCommit(oldAutoCommitSetting);
                    connection.close();
                } catch (SQLException ex) {
                    log.error(ex);
                } finally {
                    connection = null;
                }
            }
        }
    }
    
    private void fireEarlyCommit() {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.earlyCommit(this, context.getBatch());
            }
            // update the filter milliseconds so batch listeners are also
            // included
            stats.setFilterMillis(stats.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    private void fireBatchComplete() {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchComplete(this, context.getBatch());
            }
            // update the filter milliseconds so batch listeners are also
            // included
            stats.setFilterMillis(stats.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    private void fireBatchCommitted() {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchCommitted(this, context.getBatch());
            }
            // update the filter milliseconds so batch listeners are also
            // included
            stats.setFilterMillis(stats.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }

    private void fireBatchRolledback(Exception ex) {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.batchRolledback(this, context.getBatch(), ex);
            }
            // update the filter milliseconds so batch listeners are also
            // included
            stats.setFilterMillis(stats.getFilterMillis() + (System.currentTimeMillis() - ts));
        }
    }


    public IDataLoaderContext getContext() {
        return context;
    }

    public IDataLoaderStatistics getStatistics() {
        return stats;
    }
    
    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

    public void setStatisticManager(IStatisticManager statisticManager) {
        this.statisticManager = statisticManager;
    }
    
}