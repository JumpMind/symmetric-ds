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

package org.jumpmind.symmetric.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.jumpmind.symmetric.util.ArgTypePreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * An instance of TableTemplate knows how to insert, update, and delete from a
 * single table. It uses metadata from the IDbDialect to get the columns for the
 * table in the target database in case the table is missing or has missing
 * columns. It uses a cache of StatementBuilder objects that contain the SQL and
 * PreparedStatement.
 */
public class TableTemplate {

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    private Table table;

    private String schema;

    private String catalog;

    private String tableName;

    private String[] keyNames;

    private String[] columnNames;

    private String[] oldData;

    private Map<String, Column> allMetaData;

    private HashMap<DmlType, StatementBuilder> statementMap;

    private List<IColumnFilter> columnFilters = new ArrayList<IColumnFilter>();

    private boolean dontIncludeKeysInUpdateStatement = false;

    public TableTemplate(JdbcTemplate jdbcTemplate, IDbDialect dbDialect, String tableName,
            List<IColumnFilter> columnFilters, boolean dontIncludeKeysInUpdateStatement,
            String schema, String catalog) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.tableName = tableName;
        this.schema = StringUtils.isBlank(schema) ? dbDialect.getDefaultSchema() : schema;
        this.catalog = StringUtils.isBlank(catalog) ? null : catalog;
        this.setupColumnFilters(columnFilters, dbDialect);
        this.dontIncludeKeysInUpdateStatement = dontIncludeKeysInUpdateStatement;
        resetMetaData(true);
    }

    public void resetMetaData(boolean useCache) {
        table = dbDialect.getTable(catalog, schema, tableName, useCache);
        allMetaData = new HashMap<String, Column>();
        statementMap = new HashMap<DmlType, StatementBuilder>();

        if (table != null) {
            for (Column column : table.getColumns()) {
                allMetaData.put(column.getName().trim().toUpperCase(), column);
            }
        }
    }

    private void setupColumnFilters(List<IColumnFilter> pluginFilters, IDbDialect dbDialect) {
        if (pluginFilters != null) {
            for (IColumnFilter columnFilter : pluginFilters) {
                this.columnFilters.add(columnFilter);
            }
        }
        IColumnFilter filter = dbDialect.newDatabaseColumnFilter();
        if (filter != null) {
            this.columnFilters.add(filter);
        }
    }

    public String getTableName() {
        return tableName;
    }

    public boolean isIgnoreThisTable() {
        return table == null;
    }

    public int insert(IDataLoaderContext ctx, String[] columnValues) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.INSERT);
        return execute(ctx, st, columnValues);
    }

    public int update(IDataLoaderContext ctx, String[] columnValues, String[] keyValues) {
        try {
            StatementBuilder st = null;
            ArrayList<String> changedColumnNameList = new ArrayList<String>();
            ArrayList<String> changedColumnValueList = new ArrayList<String>();
            ArrayList<Column> changedColumnMetaList = new ArrayList<Column>();
            for (int i = 0; i < columnValues.length; i++) {
                Column column = allMetaData.get(columnNames[i].trim().toUpperCase());
                if (column != null) {
                    if (doesColumnNeedUpdated(ctx, i, column, keyValues, columnValues)) {
                        changedColumnNameList.add(columnNames[i]);
                        changedColumnMetaList.add(column);
                        changedColumnValueList.add(columnValues[i]);
                    }
                }
            }
            if (changedColumnNameList.size() > 0) {
                st = createStatementBuilder(ctx, DmlType.UPDATE, changedColumnNameList
                        .toArray(new String[changedColumnNameList.size()]));
                columnValues = (String[]) changedColumnValueList
                        .toArray(new String[changedColumnValueList.size()]);
                String[] values = (String[]) ArrayUtils.addAll(columnValues, keyValues);
                return execute(ctx, st, values);
            } else {
                // There was no change to apply
                return 1;
            }
        } finally {
            oldData = null;
        }

    }

    protected boolean doesColumnNeedUpdated(IDataLoaderContext ctx, int columnIndex, Column column,
            String[] keyValues, String[] columnValues) {
        boolean needsUpdated = true;
        if (oldData != null) {
            needsUpdated = !StringUtils.equals(columnValues[columnIndex], oldData[columnIndex])
                    || (dbDialect.isLob(column.getTypeCode()) && (dbDialect.needsToSelectLobData() || StringUtils
                            .isBlank(oldData[columnIndex])));
        } else if (dontIncludeKeysInUpdateStatement) {
            // This is in support of creating update statements that don't use
            // the keys in the set portion of the update statement. </p> In
            // oracle (and maybe not only in oracle) if there is no index on
            // child table on FK column and update is performing on PK on master
            // table, table lock is acquired on child table. Table lock is taken
            // not in exclusive mode, but lock contentions is possible.
            //             
            // @see ParameterConstants#DATA_LOADER_NO_KEYS_IN_UPDATE
            needsUpdated = !column.isPrimaryKey()
                    || !StringUtils.equals(columnValues[columnIndex], getKeyValue(ctx, column, keyValues));
        }
        return needsUpdated;
    }

    protected String getKeyValue(IDataLoaderContext ctx, Column column, String[] keyValues) {
        int index = ctx.getKeyIndex(column.getName());
        if (index >= 0 && keyValues != null && keyValues.length > index) {
            return keyValues[index];
        } else {
            return null;
        }
    }

    public int delete(IDataLoaderContext ctx, String[] keyValues) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.DELETE);
        return execute(ctx, st, keyValues);
    }

    public int count(IDataLoaderContext ctx, String[] keyValues) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.COUNT);
        Object[] objectValues = dbDialect.getObjectValues(ctx.getBinaryEncoding(), keyValues,
                st.getKeys());
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, st.getDmlType(), getTable(),
                        objectValues);
            }
        }
        return jdbcTemplate.queryForInt(st.getSql(), objectValues, st.getTypes());
    }

    private StatementBuilder getStatementBuilder(IDataLoaderContext ctx, DmlType type) {
        StatementBuilder st = statementMap.get(type);
        if (st == null) {
            st = createStatementBuilder(ctx, type, columnNames);
            statementMap.put(type, st);
        }
        return st;
    }

    private StatementBuilder createStatementBuilder(IDataLoaderContext ctx, DmlType type,
            String[] filteredColumnNames) {
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                filteredColumnNames = columnFilter.filterColumnsNames(ctx, type, getTable(),
                        filteredColumnNames);
            }
        }

        String tableName = table.getName();
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return new StatementBuilder(type, tableName, getColumnMetaData(keyNames),
                getColumnMetaData(filteredColumnNames), dbDialect.isDateOverrideToTimestamp(),
                dbDialect.getIdentifierQuoteString());
    }

    public Object[] getObjectValues(IDataLoaderContext ctx, String[] values) {
        return dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, getColumnMetaData(columnNames));
    }

    public Object[] getObjectKeyValues(IDataLoaderContext ctx, String[] values) {
        return dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, getColumnMetaData(keyNames));
    }

    private int execute(IDataLoaderContext ctx, StatementBuilder st, String[] values) {
        Object[] objectValues = dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, st
                .getMetaData());
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, st.getDmlType(), getTable(),
                        objectValues);
            }
        }
        return jdbcTemplate.update(st.getSql(), new ArgTypePreparedStatementSetter(objectValues, st
                .getTypes(), dbDialect.getLobHandler()));
    }

    public void setKeyNames(String[] keyNames) {
        this.keyNames = keyNames;
        clear();
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
        clear();
    }

    public void setOldData(String[] oldData) {
        this.oldData = oldData;
    }

    private void clear() {
        statementMap.clear();
        oldData = null;
    }

    private Column[] getColumnMetaData(String[] names) {
        Column[] columns = new Column[names.length];
        for (int i = 0; i < names.length; i++) {
            columns[i] = allMetaData.get(names[i].trim().toUpperCase());
        }
        return columns;
    }

    public String[] getKeyNames() {
        return keyNames;
    }

    public String[] getColumnNames() {
        return columnNames;
    }

    public Table getTable() {
        return table;
    }

    public String[] getOldData() {
        return oldData;
    }

}
