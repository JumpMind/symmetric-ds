/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.load.csv;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ErrorConstants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.DataLoaderStatistics;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.load.TableTemplate;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.ITriggerRouterService;
import org.jumpmind.symmetric.util.CsvUtils;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import bsh.EvalError;
import bsh.Interpreter;

import com.csvreader.CsvReader;

public class CsvLoader implements IDataLoader {

    static final ILog log = LogFactory.getLog(CsvLoader.class);

    protected JdbcTemplate jdbcTemplate;

    protected IDbDialect dbDialect;

    protected IParameterService parameterService;

    protected ITriggerRouterService triggerRouterService;

    protected INodeService nodeService;

    protected CsvReader csvReader;

    protected DataLoaderContext context;

    protected DataLoaderStatistics stats;

    protected List<IDataLoaderFilter> filters;

    protected Map<String, IColumnFilter> columnFilters;

    public void open(BufferedReader reader) throws IOException {
        csvReader = CsvUtils.getCsvReader(reader);
        context = new DataLoaderContext();
        stats = new DataLoaderStatistics();
    }

    public void open(BufferedReader reader, List<IDataLoaderFilter> filters, Map<String, IColumnFilter> columnFilters)
            throws IOException {
        open(reader);
        this.filters = filters;
        this.columnFilters = columnFilters;
    }

    public boolean hasNext() throws IOException {
        while (csvReader.readRecord()) {
            String[] tokens = csvReader.getValues();

            if (tokens[0].equals(CsvConstants.BATCH)) {
                context.setBatchId(new Long(tokens[1]));
                stats = new DataLoaderStatistics();
                return true;
            } else if (tokens[0].equals(CsvConstants.NODEID)) {
                context.setNodeId(tokens[1]);
            } else if (tokens[0].equals(CsvConstants.VERSION)) {
                context.setVersion(tokens[1] + "." + tokens[2] + "." + tokens[3]);
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

    public boolean load() throws IOException {
        try {
            long rowsBeforeCommit = parameterService.getLong(ParameterConstants.DATA_LOADER_MAX_ROWS_BEFORE_COMMIT);
            long rowsProcessed = 0;
            prepareTableForDataLoad();
            while (csvReader.readRecord()) {
                String[] tokens = csvReader.getValues();
                stats.incrementLineCount();
                if (tokens != null && tokens.length > 0 && tokens[0] != null) {
                    stats.incrementByteCount(csvReader.getRawRecord().length());

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
                            runBsh(tokens[1]);
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
                    return false;
                }
            }

            return true;
        } finally {
            cleanupAfterDataLoad();
        }
    }
    
    protected void runBsh(String script) {
        try {
            Interpreter interpreter =  new Interpreter();
            interpreter.eval(script);
        } catch (EvalError e) {
            throw new RuntimeException(e);
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
            Object savepoint = null;
            try {
                stats.startTimer();
                if (enableFallbackUpdate) {
                    savepoint = dbDialect.createSavepointForFallback();
                }
                rows = context.getTableTemplate().insert(context, columnValues);
                dbDialect.releaseSavepoint(savepoint);
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
                        throw new SymmetricException("LoaderUpdatingFailed", e, context.getTableName(), ArrayUtils
                                .toString(tokens));
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
        dataLoader.setJdbcTemplate(jdbcTemplate);
        dataLoader.setDbDialect(dbDialect);
        dataLoader.setParameterService(parameterService);
        dataLoader.setTriggerRouterService(triggerRouterService);
        dataLoader.setNodeService(nodeService);
        return dataLoader;
    }

    public void close() {
        if (csvReader != null) {
            csvReader.close();
        }
    }

    public IDataLoaderContext getContext() {
        return context;
    }

    public IDataLoaderStatistics getStatistics() {
        return stats;
    }

    public void setJdbcTemplate(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setDbDialect(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    public void setParameterService(IParameterService parameterService) {
        this.parameterService = parameterService;
    }

    public void setTriggerRouterService(ITriggerRouterService triggerService) {
        this.triggerRouterService = triggerService;
    }

    public void setNodeService(INodeService nodeService) {
        this.nodeService = nodeService;
    }

}
