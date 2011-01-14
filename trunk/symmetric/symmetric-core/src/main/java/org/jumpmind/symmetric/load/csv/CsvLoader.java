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
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
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
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.jumpmind.symmetric.statistic.StatisticConstants;
import org.jumpmind.symmetric.util.AppUtils;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.dao.DataIntegrityViolationException;
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

    public void open(BufferedReader reader, DataSource dataSource, List<IBatchListener> batchListeners) throws IOException {
        try {
            this.connection = dataSource.getConnection();
            this.oldAutoCommitSetting = this.connection.getAutoCommit();
            this.connection.setAutoCommit(false);
            this.batchListeners = batchListeners != null ? batchListeners : new ArrayList<IBatchListener>();
            this.jdbcTemplate = new JdbcTemplate(new SingleConnectionDataSource(connection, true));
            this.jdbcTemplate.setQueryTimeout(parameterService.getInt(ParameterConstants.DB_QUERY_TIMEOUT_SECS));
            this.csvReader = CsvUtils.getCsvReader(reader);
            this.context = new DataLoaderContext(nodeService, jdbcTemplate);
            this.stats = new DataLoaderStatistics();
        } catch (SQLException ex) {
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
        while (csvReader.readRecord()) {
            String[] tokens = csvReader.getValues();
            if (tokens[0].equals(CsvConstants.BATCH)) {
                this.context.setBatchId(new Long(tokens[1]));
                this.stats = new DataLoaderStatistics();
                return true;
            } else if (tokens[0].equals(CsvConstants.NODEID)) {
                this.context.setNodeId(tokens[1]);
            } else if (isMetaTokenParsed(tokens)) {
                continue;
            } else {
                throw new RuntimeException("Unexpected token '" + tokens[0] + "' while parsing for next batch");
            }
        }
        return false;
    }

    public void skip() throws IOException {
        context.setSkipping(true);
        load();
        // skipping is reset when a new batch_id is set
    }

    public void load() throws IOException {
        try {
            long rowsBeforeCommit = parameterService.getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT);
            long rowsProcessed = 0;
            lineCount = 0;
            bytesCount = 0;
            prepareTableForDataLoad();
            while (csvReader.readRecord()) {
                String[] tokens = csvReader.getValues();
                if (tokens != null && tokens.length > 0 && tokens[0] != null) {
                    stats.incrementLineCount();
                    lineCount++;
                    long numberOfBytes = csvReader.getRawRecord().length();
                    stats.incrementByteCount(numberOfBytes);
                    bytesCount += numberOfBytes;                     
                    if (tokens[0].equals(CsvConstants.INSERT)) {
                        if (context.getTableTemplate() == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                            insert(tokens);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.UPDATE)) {
                        if (context.getTableTemplate() == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                            update(tokens);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.DELETE)) {
                        if (context.getTableTemplate() == null) {
                            throw new IllegalStateException(ErrorConstants.METADATA_MISSING);
                        } else if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                            delete(tokens);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.OLD)) {
                        context.setOldData((String[]) ArrayUtils.subarray(tokens, 1, tokens.length));
                    } else if (isMetaTokenParsed(tokens)) {
                        continue;
                    } else if (tokens[0].equals(CsvConstants.COMMIT)) {
                        fireBatchComplete();
                        connection.commit();
                        fireBatchCommitted();
                        rowsProcessed = 0;
                        break;
                    } else if (tokens[0].equals(CsvConstants.SQL)) {
                        if ((context.getTableTemplate() == null || !context.getTableTemplate().isIgnoreThisTable())
                                && !context.isSkipping()) {
                            runSql(tokens[1]);
                            rowsProcessed++;
                        }
                    } else if (tokens[0].equals(CsvConstants.BSH)) {
                        if (!context.isSkipping()) {
                            AppUtils.runBsh(tokens[1]);
                            rowsProcessed++;
                        }                        
                    } else if (tokens[0].equals(CsvConstants.CREATE)) {
                        if (!context.isSkipping()) {
                            runDdl(tokens[1]);
                            rowsProcessed++;
                        }
                    } else {
                        log.warn("LoaderTokenUnexpected", tokens[0], stats.getLineCount(), context.getBatchId());
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

            }

        } catch (RuntimeException ex) {
            rollback(ex);            
            throw ex;
        } catch (Exception ex) {
            rollback(ex);
            throw new RuntimeException(ex);
        } finally {
            if (bytesCount > 0) {
                statisticManager.incrementDataBytesLoaded(context.getChannelId(), bytesCount);
            }
            
            if (lineCount > 0) {
                statisticManager.incrementDataLoaded(context.getChannelId(), lineCount);
            }
            cleanupAfterDataLoad();
        }
    }   
    
    protected void rollback(Exception ex) {
        try {
            connection.rollback();
        } catch (SQLException ex2) {
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
            resetTable(tokens[1]);
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

    protected void resetTable(String tableName) {
        cleanupAfterDataLoad();
        context.setTableName(tableName);
        if (context.getTableTemplate() == null) {
            context.setTableTemplate(new TableTemplate(jdbcTemplate, dbDialect, tableName,
                    this.columnFilters != null ? this.columnFilters.get(tableName) : null, parameterService
                            .is(ParameterConstants.DATA_LOADER_NO_KEYS_IN_UPDATE), context.getSchemaName(), context.getCatalogName()));
        }
        prepareTableForDataLoad();
    }

    protected void prepareTableForDataLoad() {
        if (context != null && context.getTableTemplate() != null) {
            dbDialect.prepareTableForDataLoad(context.getTableTemplate().getTable());
        }
    }

    protected void cleanupAfterDataLoad() {
        if (context != null && context.getTableTemplate() != null) {
            dbDialect.cleanupAfterDataLoad(context.getTableTemplate().getTable());
        }
    }

    protected int insert(String[] tokens) {
        stats.incrementStatementCount();
        String[] columnValues = parseColumns(tokens, 1);
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
            boolean enableFallbackUpdate = parameterService.is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_UPDATE);
            boolean enableFallbackSavepoint = parameterService.is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_SAVEPOINT);
            Object savepoint = null;
            try {
                stats.startTimer();
                if (enableFallbackUpdate && dbDialect.requiresSavepointForFallback()) {
                    if (enableFallbackSavepoint) {
                        savepoint = dbDialect.createSavepointForFallback();
                    } else if (context.getTableTemplate().count(context, parseKeys(tokens, 1)) > 0) {
                        throw new DataIntegrityViolationException("Row already exists");
                    }
                }
                rows = context.getTableTemplate().insert(context, columnValues);
            } catch (DataIntegrityViolationException e) {
                // TODO: modify sql-error-codes.xml for unique constraint vs
                // foreign key
                if (enableFallbackUpdate) {
                    dbDialect.rollbackToSavepoint(savepoint);
                    if (log.isDebugEnabled()) {
                        log.debug("LoaderInsertingFailedUpdating", context.getTableName(), ArrayUtils.toString(tokens));
                    }
                    String keyValues[] = parseKeys(tokens, 1);
                    stats.incrementFallbackUpdateCount();
                    rows = context.getTableTemplate().update(context, columnValues, keyValues);
                    if (rows == 0) {
                        throw new SymmetricException("LoaderFallbackUpdateFailed", e, context.getTableName(), ArrayUtils
                                .toString(tokens), ArrayUtils.toString(keyValues));
                    }
                } else {
                    log.error("LoaderInsertingFailed", context.getTableName(), ArrayUtils.toString(tokens));
                    throw e;
                }
            } finally {
                stats.incrementDatabaseMillis(stats.endTimer());
            }           
        }
        return rows;
    }

    protected int update(String[] tokens) {
        stats.incrementStatementCount();
        String columnValues[] = parseColumns(tokens, 1);
        String keyValues[] = parseKeys(tokens, 1 + columnValues.length);
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
            boolean enableFallbackInsert = parameterService.is(ParameterConstants.DATA_LOADER_ENABLE_FALLBACK_INSERT);
            stats.startTimer();
            rows = context.getTableTemplate().update(context, columnValues, keyValues);
            if (rows == 0) {
                if (enableFallbackInsert) {

                    log.debug("LoaderUpdatingFailedInserting", context.getTableName(), ArrayUtils.toString(tokens));

                    stats.incrementFallbackInsertCount();
                    rows = context.getTableTemplate().insert(context, columnValues);
                } else {
                    stats.incrementDatabaseMillis(stats.endTimer());
                    throw new SymmetricException("LoaderUpdatingFailed", context.getTableName(), ArrayUtils
                            .toString(tokens));
                }
            } else if (rows > 1) {
                log.warn("LoaderRowsUpdatingFailed", rows, context.getTableName(), ArrayUtils.toString(tokens));
            }
            stats.incrementDatabaseMillis(stats.endTimer());
        }
        return rows;
    }

    protected int delete(String[] tokens) {
        stats.incrementStatementCount();
        String keyValues[] = parseKeys(tokens, 1);
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

    protected String[] parseKeys(String[] tokens, int startIndex) {
        if (context.getTableTemplate().getKeyNames() == null) {
            throw new RuntimeException("Key names were not specified for table "
                    + context.getTableTemplate().getTableName());
        }
        int keyLength = context.getTableTemplate().getKeyNames().length;
        return parseValues("key", tokens, startIndex, startIndex + keyLength);
    }

    protected String[] parseColumns(String[] tokens, int startIndex) {
        if (context.getTableTemplate().getColumnNames() == null) {
            throw new RuntimeException("Column names were not specified for table "
                    + context.getTableTemplate().getTableName());
        }
        int columnLength = context.getTableTemplate().getColumnNames().length;
        return parseValues("column", tokens, startIndex, startIndex + columnLength);
    }

    protected String[] parseValues(String name, String[] tokens, int startIndex, int endIndex) {
        if (tokens.length < endIndex) {
            throw new RuntimeException("Expected to have " + (endIndex - startIndex) + " " + name + " values for "
                    + context.getTableTemplate().getTableName() + ": " + ArrayUtils.toString(tokens));
        }
        return (String[]) ArrayUtils.subarray(tokens, startIndex, endIndex);
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
        if (csvReader != null) {
            csvReader.close();
        }
        
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
    
    private void fireEarlyCommit() {
        if (batchListeners != null) {
            long ts = System.currentTimeMillis();
            for (IBatchListener listener : batchListeners) {
                listener.earlyCommit(this, context);
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
                listener.batchComplete(this, context);
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
                listener.batchCommitted(this, context);
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
                listener.batchRolledback(this, context, ex);
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