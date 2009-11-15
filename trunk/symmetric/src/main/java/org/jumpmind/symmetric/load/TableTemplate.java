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
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
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

    private Column[] keyMetaData;

    private Column[] columnMetaData;

    private Column[] columnKeyMetaData;

    private Column[] noKeyColumnPlusKeyMetaData;

    private HashMap<DmlType, StatementBuilder> statementMap;

    private int[] keyIndexesToRemoveOnUpdate;

    private List<IColumnFilter> columnFilters = new ArrayList<IColumnFilter>();

    private boolean dontIncludeKeysInUpdateStatement = false;

    public TableTemplate(JdbcTemplate jdbcTemplate, IDbDialect dbDialect, String tableName, IColumnFilter columnFilter,
            boolean dontIncludeKeysInUpdateStatement) {
        this(jdbcTemplate, dbDialect, tableName, columnFilter, dontIncludeKeysInUpdateStatement, null, null);
    }
    
    public TableTemplate(JdbcTemplate jdbcTemplate, IDbDialect dbDialect, String tableName, IColumnFilter columnFilter,
            boolean dontIncludeKeysInUpdateStatement, String schema, String catalog) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.tableName = tableName;
        this.schema = StringUtils.isBlank(schema) ?dbDialect.getDefaultSchema() : schema;
        this.catalog = StringUtils.isBlank(catalog) ? null : catalog;
        this.setupColumnFilters(columnFilter, dbDialect);
        this.dontIncludeKeysInUpdateStatement = dontIncludeKeysInUpdateStatement;
        resetMetaData(true);
    }

    public void resetMetaData(boolean useCache) {
        table = dbDialect.getTable(catalog, schema, tableName, useCache);
        allMetaData = new HashMap<String, Column>();
        statementMap = new HashMap<DmlType, StatementBuilder>();
        keyMetaData = null;
        columnMetaData = null;
        columnKeyMetaData = null;

        if (table != null) {
            for (Column column : table.getColumns()) {
                allMetaData.put(column.getName().trim().toUpperCase(), column);
            }
        }
    }

    private void setupColumnFilters(IColumnFilter pluginFilter, IDbDialect dbDialect) {
        if (pluginFilter != null) {
            this.columnFilters.add(pluginFilter);
        }
        if (dbDialect.getDatabaseColumnFilter() != null) {
            this.columnFilters.add(dbDialect.getDatabaseColumnFilter());
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
        return execute(ctx, st, columnValues, columnMetaData);
    }

    public int update(IDataLoaderContext ctx, String[] columnValues, String[] keyValues) {
        StatementBuilder st = null;
        Column[] metaData = null;
        if (oldData != null) {
            ArrayList<String> changedColumnNameList = new ArrayList<String>();
            ArrayList<String> changedColumnValueList = new ArrayList<String>();
            ArrayList<Column> changedColumnMetaList = new ArrayList<Column>();
            for (int i = 0; i < columnValues.length; i++) {
                if (! StringUtils.equals(columnValues[i], oldData[i])) {
                    changedColumnNameList.add(columnNames[i]);
                    changedColumnMetaList.add(allMetaData.get(columnNames[i].trim().toUpperCase()));
                    changedColumnValueList.add(columnValues[i]);
                }
            }
            if (changedColumnNameList.size() > 0) {
                String[] changedColumnNames = changedColumnNameList.toArray(new String[0]);
                st = createStatementBuilder(ctx, DmlType.UPDATE, changedColumnNames);
                columnValues = (String[]) changedColumnValueList.toArray(new String[0]);
                Column[] changedColumnMetaData = (Column[]) changedColumnMetaList.toArray(new Column[0]);
                metaData = (Column[]) ArrayUtils.addAll(changedColumnMetaData, keyMetaData);
            }
            oldData = null;
        } else if (dontIncludeKeysInUpdateStatement) {
            String[] values = removeKeysFromColumnValuesIfSame(ctx, keyValues, columnValues);
            if (values != null) {
                columnValues = values;
                st = getStatementBuilder(ctx, DmlType.UPDATE_NO_KEYS);
                metaData = noKeyColumnPlusKeyMetaData;
            }
        }

        if (st == null) {
            st = getStatementBuilder(ctx, DmlType.UPDATE);
            metaData = columnKeyMetaData;
        }
        String[] values = (String[]) ArrayUtils.addAll(columnValues, keyValues);
        return execute(ctx, st, values, metaData);
    }

    /**
     * This is in support of creating update statements that don't use the keys
     * in the set portion of the update statement.
     * </p>
     * In oracle (and maybe not only in oracle) if there is no index on child table 
     * on FK column and update is performing on PK on master table, table lock is acquired 
     * on child table. Table lock is taken not in exclusive mode, but lock contentions 
     * is possible. 
     * @see ParameterConstants#DATA_LOADER_NO_KEYS_IN_UPDATE
     */
    private String[] removeKeysFromColumnValuesIfSame(IDataLoaderContext ctx, String[] keyValues, String[] columnValues) {
        if (keyIndexesToRemoveOnUpdate == null) {
            String[] colNames = ctx.getColumnNames();
            String[] keyNames = ctx.getKeyNames();
            String[] noKeyColNames = new String[colNames.length - keyNames.length];
            if (noKeyColNames.length > 0) {
                keyIndexesToRemoveOnUpdate = new int[keyNames.length];
                int indexToRemoveIndex = 0;
                int indexOfNoKeyColNames = 0;
                for (int index = 0; index < colNames.length; index++) {
                    if (ArrayUtils.contains(keyNames, colNames[index])) {
                        keyIndexesToRemoveOnUpdate[indexToRemoveIndex++] = index;
                    } else {
                        noKeyColNames[indexOfNoKeyColNames++] = colNames[index];
                    }
                }
            } else {
                keyIndexesToRemoveOnUpdate = new int[0];
            }
            noKeyColumnPlusKeyMetaData = getColumnMetaData((String[]) ArrayUtils.addAll(noKeyColNames, keyNames));

        }

        if (keyIndexesToRemoveOnUpdate.length > 0) {
            if (noKeyColumnPlusKeyMetaData == null) {
                String[] noKeys = new String[columnValues.length - keyValues.length];
                int noKeysIndex = 0;
                for (int index = 0; index < columnValues.length; index++) {
                    if (!ArrayUtils.contains(keyIndexesToRemoveOnUpdate, index)) {
                        noKeys[noKeysIndex++] = columnValues[index];
                    }
                }
                return noKeys;
            }

            boolean keyChanged = false;
            for (int index = 0; index < keyIndexesToRemoveOnUpdate.length; index++) {
                if (!StringUtils.equals(keyValues[index], columnValues[index])) {
                    keyChanged = true;
                }
            }

            if (!keyChanged) {
                String[] noKeys = new String[columnValues.length - keyValues.length];
                int noKeysIndex = 0;
                for (int index = 0; index < columnValues.length; index++) {
                    if (!ArrayUtils.contains(keyIndexesToRemoveOnUpdate, index)) {
                        noKeys[noKeysIndex++] = columnValues[index];
                    }
                }
                return noKeys;

            }
        }
        return null;
    }

    public int delete(IDataLoaderContext ctx, String[] keyValues) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.DELETE);
        return execute(ctx, st, keyValues, keyMetaData);
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
                filteredColumnNames = columnFilter.filterColumnsNames(ctx, type, getTable(), filteredColumnNames);
            }
        }
        if (keyMetaData == null) {
            keyMetaData = getColumnMetaData(keyNames);
        }
        if (columnMetaData == null) {
            columnMetaData = getColumnMetaData(columnNames);
        }
        if (columnKeyMetaData == null) {
            columnKeyMetaData = (Column[]) ArrayUtils.addAll(columnMetaData, keyMetaData);
        }

        String tableName = table.getName();
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return new StatementBuilder(type, tableName, keyMetaData, getColumnMetaData(filteredColumnNames),
                dbDialect.isBlobOverrideToBinary(), dbDialect.isDateOverrideToTimestamp(), dbDialect
                        .getIdentifierQuoteString());
    }
    
    public Object[] getObjectValues(IDataLoaderContext ctx, String[] values) {
        return dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, columnMetaData == null ? getColumnMetaData(columnNames) : columnMetaData);
    }
    
    public Object[] getObjectKeyValues(IDataLoaderContext ctx, String[] values) {
        return dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, keyMetaData == null ? getColumnMetaData(keyNames) : keyMetaData);
    }        

    private int execute(IDataLoaderContext ctx, StatementBuilder st, String[] values, Column[] metaData) {        
        Object[] objectValues = dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, metaData);
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, st.getDmlType(), getTable(), objectValues);
            }
        }
        return jdbcTemplate.update(st.getSql(), objectValues, st.getTypes());
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
        keyMetaData = null;
        columnMetaData = null;
        columnKeyMetaData = null;
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
