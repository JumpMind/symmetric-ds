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
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.common.csv.CsvConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.DataLoaderContext;
import org.jumpmind.symmetric.load.DataLoaderStatistics;
import org.jumpmind.symmetric.load.IColumnFilter;
import org.jumpmind.symmetric.load.IDataLoader;
import org.jumpmind.symmetric.load.IDataLoaderContext;
import org.jumpmind.symmetric.load.IDataLoaderFilter;
import org.jumpmind.symmetric.load.IDataLoaderStatistics;
import org.jumpmind.symmetric.load.TableTemplate;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.jdbc.core.JdbcTemplate;

import com.csvreader.CsvReader;

public class CsvLoader implements IDataLoader {

    static final Log logger = LogFactory.getLog(CsvLoader.class);

    protected JdbcTemplate jdbcTemplate;

    protected IDbDialect dbDialect;

    protected CsvReader csvReader;

    protected DataLoaderContext context;

    protected DataLoaderStatistics stats;

    protected boolean enableFallbackInsert;

    protected boolean enableFallbackUpdate;

    protected boolean allowMissingDelete;

    protected List<IDataLoaderFilter> filters;

    protected Map<String, IColumnFilter> columnFilters;

    public void open(final BufferedReader reader) throws IOException {
        csvReader = new CsvReader(reader);
        csvReader.setEscapeMode(CsvReader.ESCAPE_MODE_BACKSLASH);
        context = new DataLoaderContext();
        stats = new DataLoaderStatistics();
    }

    public void open(final BufferedReader reader, final List<IDataLoaderFilter> filters,
            final Map<String, IColumnFilter> columnFilters) throws IOException {
        open(reader);
        this.filters = filters;
        this.columnFilters = columnFilters;
    }

    public boolean hasNext() throws IOException {
        while (csvReader.readRecord()) {
            final String[] tokens = csvReader.getValues();

            if (tokens[0].equals(CsvConstants.BATCH)) {
                context.setBatchId(tokens[1]);
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
        try {
            context.setSkipping(true);
            load();
        } finally {
            context.setSkipping(false);
        }
    }

    public void load() throws IOException {
        while (csvReader.readRecord()) {
            final String[] tokens = csvReader.getValues();
            stats.incrementLineCount();

            if (tokens[0].equals(CsvConstants.INSERT)) {
                if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                    insert(tokens);
                }
            } else if (tokens[0].equals(CsvConstants.UPDATE)) {
                if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                    update(tokens);
                }
            } else if (tokens[0].equals(CsvConstants.DELETE)) {
                if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                    delete(tokens);
                }
            } else if (isMetaTokenParsed(tokens)) {
                continue;
            } else if (tokens[0].equals(CsvConstants.COMMIT)) {
                break;
            } else if (tokens[0].equals(CsvConstants.SQL)) {
                if (!context.getTableTemplate().isIgnoreThisTable() && !context.isSkipping()) {
                    runSql(tokens);
                }
            } else {
                throw new RuntimeException("Unexpected token '" + tokens[0] + "' on line " + stats.getLineCount()
                        + " of batch " + context.getBatchId());
            }
        }
    }

    protected boolean isMetaTokenParsed(final String[] tokens) {
        boolean isMetaTokenParsed = true;
        if (tokens[0].equals(CsvConstants.TABLE)) {
            setTable(tokens[1].toLowerCase());
        } else if (tokens[0].equals(CsvConstants.KEYS)) {
            context.setKeyNames((String[]) ArrayUtils.subarray(tokens, 1, tokens.length));
        } else if (tokens[0].equals(CsvConstants.COLUMNS)) {
            context.setColumnNames((String[]) ArrayUtils.subarray(tokens, 1, tokens.length));
        } else {
            isMetaTokenParsed = false;
        }
        return isMetaTokenParsed;
    }

    protected void setTable(final String tableName) {
        context.setTableName(tableName);
        if (context.getTableTemplate() == null) {
            context.setTableTemplate(new TableTemplate(jdbcTemplate, dbDialect, tableName,
                    this.columnFilters != null ? this.columnFilters.get(tableName) : null));
        }
    }

    protected int insert(final String[] tokens) {
        stats.incrementStatementCount();
        final String[] columnValues = parseColumns(tokens, 1);
        int rows = 0;

        if (filters != null) {
            for (final IDataLoaderFilter filter : filters) {
                filter.filterInsert(context, columnValues);
            }
        }

        try {
            rows = context.getTableTemplate().insert(columnValues);
        } catch (final DataIntegrityViolationException e) {
            // TODO: modify sql-error-codes.xml for unique constraint vs foreign key
            if (enableFallbackUpdate) {
                if (logger.isDebugEnabled()) {
                    logger.debug("Unable to insert into " + context.getTableName() + ", updating instead: "
                            + ArrayUtils.toString(tokens));
                }
                final String keyValues[] = parseKeys(tokens, 1);
                stats.incrementFallbackUpdateCount();
                rows = context.getTableTemplate().update(columnValues, keyValues);
                if (rows == 0) {
                    throw new RuntimeException("Unable to update " + context.getTableName() + ": "
                            + ArrayUtils.toString(tokens));
                }
            } else {
                throw e;
            }
        }
        return rows;
    }

    protected int update(final String[] tokens) {
        stats.incrementStatementCount();
        final String columnValues[] = parseColumns(tokens, 1);
        final String keyValues[] = parseKeys(tokens, 1 + columnValues.length);

        if (filters != null) {
            for (final IDataLoaderFilter filter : filters) {
                filter.filterUpdate(context, columnValues, keyValues);
            }
        }

        final int rows = context.getTableTemplate().update(columnValues, keyValues);
        if (rows == 0) {
            if (enableFallbackInsert) {
                if (logger.isDebugEnabled()) {
                    logger.warn("Unable to update " + context.getTableName() + ", inserting instead: "
                            + ArrayUtils.toString(tokens));
                }
                stats.incrementFallbackInsertCount();
                return context.getTableTemplate().insert(columnValues);
            } else {
                throw new RuntimeException("Unable to update " + context.getTableName() + ": "
                        + ArrayUtils.toString(tokens));
            }
        } else if (rows > 1) {
            logger.warn("Too many rows (" + rows + ") updated for " + context.getTableName() + ": "
                    + ArrayUtils.toString(tokens));
        }
        return rows;
    }

    protected int delete(final String[] tokens) {
        stats.incrementStatementCount();
        final String keyValues[] = parseKeys(tokens, 1);

        if (filters != null) {
            for (final IDataLoaderFilter filter : filters) {
                filter.filterDelete(context, keyValues);
            }
        }

        final int rows = context.getTableTemplate().delete(keyValues);
        if (rows == 0) {
            if (allowMissingDelete) {
                logger
                        .warn("Delete of " + context.getTableName() + " affected no rows: "
                                + ArrayUtils.toString(tokens));
                stats.incrementMissingDeleteCount();
            } else {
                throw new RuntimeException("Delete of " + context.getTableName() + " affected no rows: "
                        + ArrayUtils.toString(tokens));
            }
        }
        return rows;
    }

    protected void runSql(final String[] tokens) {
        stats.incrementStatementCount();
        logger.debug("Running SQL: " + tokens[1]);
        jdbcTemplate.execute(tokens[1]);
    }

    protected String[] parseKeys(final String[] tokens, final int startIndex) {
        if (context.getTableTemplate().getKeyNames() == null) {
            throw new RuntimeException("Key names were not specified for table "
                    + context.getTableTemplate().getTableName());
        }
        final int keyLength = context.getTableTemplate().getKeyNames().length;
        return parseValues("key", tokens, startIndex, startIndex + keyLength);
    }

    protected String[] parseColumns(final String[] tokens, final int startIndex) {
        if (context.getTableTemplate().getColumnNames() == null) {
            throw new RuntimeException("Column names were not specified for table "
                    + context.getTableTemplate().getTableName());
        }
        final int columnLength = context.getTableTemplate().getColumnNames().length;
        return parseValues("column", tokens, startIndex, startIndex + columnLength);
    }

    protected String[] parseValues(final String name, final String[] tokens, final int startIndex, final int endIndex) {
        if (tokens.length < endIndex) {
            throw new RuntimeException("Expected to have " + (endIndex - startIndex) + " " + name + " values for "
                    + context.getTableTemplate().getTableName() + ": " + ArrayUtils.toString(tokens));
        }
        return (String[]) ArrayUtils.subarray(tokens, startIndex, endIndex);
    }

    @Override
    public IDataLoader clone() {
        final CsvLoader dataLoader = new CsvLoader();
        dataLoader.setJdbcTemplate(jdbcTemplate);
        dataLoader.setDbDialect(dbDialect);
        dataLoader.setEnableFallbackInsert(enableFallbackInsert);
        dataLoader.setEnableFallbackUpdate(enableFallbackUpdate);
        dataLoader.setAllowMissingDelete(allowMissingDelete);
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

    public boolean isAllowMissingDelete() {
        return allowMissingDelete;
    }

    public void setAllowMissingDelete(final boolean allowMissingDelete) {
        this.allowMissingDelete = allowMissingDelete;
    }

    public boolean isEnableFallbackInsert() {
        return enableFallbackInsert;
    }

    public void setEnableFallbackInsert(final boolean enableFallbackInsert) {
        this.enableFallbackInsert = enableFallbackInsert;
    }

    public boolean isEnableFallbackUpdate() {
        return enableFallbackUpdate;
    }

    public void setEnableFallbackUpdate(final boolean enableFallbackUpdate) {
        this.enableFallbackUpdate = enableFallbackUpdate;
    }

    public void setJdbcTemplate(final JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    public void setDbDialect(final IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }
}
