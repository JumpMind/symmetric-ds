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


package org.jumpmind.symmetric.load;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.ddl.model.Table;
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
    
    private String[] filteredColumnNames;

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
        this.schema = StringUtils.isBlank(schema) ? null : schema;
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

    final public boolean isIgnoreThisTable() {
        return table == null;
    }

    public int insert(IDataLoaderContext ctx, String[] columnValues) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.INSERT, columnNames);
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
                st = getStatementBuilder(ctx, DmlType.UPDATE, changedColumnNameList
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

    final private boolean doesColumnNeedUpdated(IDataLoaderContext ctx, int columnIndex, Column column,
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
        StatementBuilder st = getStatementBuilder(ctx, DmlType.DELETE, columnNames);
        return execute(ctx, st, keyValues);
    }

    public int count(IDataLoaderContext ctx, String[] keyValues) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.COUNT, columnNames);
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
    public String getFullyQualifiedTableName() {
        return getFullyQualifiedTableName(false);
    }
    
    public String getFullyQualifiedTableName(boolean preventQuotes) {
        String quote = !preventQuotes ? dbDialect.getIdentifierQuoteString() : "";
        String tableName = quote + (table != null ? table.getName() : this.tableName) + quote;
        if (!StringUtils.isBlank(schema)) {
            tableName = schema + "." + tableName;
        }
        if (!StringUtils.isBlank(catalog)) {
            tableName = catalog + "." + tableName;
        }
        return tableName;
    }

    final private StatementBuilder getStatementBuilder(IDataLoaderContext ctx, DmlType type,
            String[] preFilteredColumnNames) {
        StatementBuilder st = statementMap.get(type);
        if (st == null) {            
            this.filteredColumnNames = preFilteredColumnNames;
            if (columnFilters != null) {
                for (IColumnFilter columnFilter : columnFilters) {
                    this.filteredColumnNames = columnFilter.filterColumnsNames(ctx, type, getTable(),
                            this.filteredColumnNames);
                }
            }

            String tableName = getFullyQualifiedTableName();
            
            st = new StatementBuilder(type, tableName, getColumnMetaData(keyNames),
                    getColumnMetaData(this.filteredColumnNames),
                    getColumnMetaData(preFilteredColumnNames), dbDialect
                            .isDateOverrideToTimestamp(), dbDialect.getIdentifierQuoteString());

            if (type != DmlType.UPDATE) {
                statementMap.put(type, st);
            }
        }
        return st;
    }

    public Object[] getObjectValues(IDataLoaderContext ctx, String[] values) {
        return dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, getColumnMetaData(columnNames));
    }

    public Object[] getObjectKeyValues(IDataLoaderContext ctx, String[] values) {
        return dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, getColumnMetaData(keyNames));
    }

    final private int execute(IDataLoaderContext ctx, StatementBuilder st, String[] values) {
        Object[] objectValues = dbDialect.getObjectValues(ctx.getBinaryEncoding(), values, st
                .getMetaData(true));
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

    final private void clear() {
        statementMap.clear();
        oldData = null;
    }

    final private Column[] getColumnMetaData(String[] names) {
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
    
    public String[] getFilteredColumnNames() {
        return filteredColumnNames != null ? filteredColumnNames : columnNames;
    }

    public Table getTable() {
        return table;
    }

    public String[] getOldData() {
        return oldData;
    }

}