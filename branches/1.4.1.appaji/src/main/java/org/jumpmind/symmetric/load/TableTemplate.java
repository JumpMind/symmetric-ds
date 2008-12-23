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

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.DecoderException;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.codec.binary.Hex;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.load.StatementBuilder.DmlType;
import org.springframework.jdbc.core.JdbcTemplate;

/**
 * An instance of TableTemplate knows how to insert, update, and delete from a
 * single table. It uses metadata from the IDbDialect to get the columns for the
 * table in the target database in case the table is missing or has missing
 * columns. It uses a cache of StatementBuilder objects that contain the SQL and
 * PreparedStatement.
 * 
 * @author elong
 * 
 */
public class TableTemplate {
    public static final String REQUIRED_FIELD_NULL_SUBSTITUTE = " ";

    public static final String[] TIMESTAMP_PATTERNS = { "yyyy-MM-dd HH:mm:ss.S", "yyyy-MM-dd HH:mm:ss", "yyyy-MM-dd" };

    private JdbcTemplate jdbcTemplate;

    private IDbDialect dbDialect;

    private Table table;

    private String schema;

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
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.setupColumnFilters(columnFilter, dbDialect);
        this.dontIncludeKeysInUpdateStatement = dontIncludeKeysInUpdateStatement;

        int periodIndex = tableName.indexOf(".");
        if (periodIndex != -1) {
            this.schema = tableName.substring(0, periodIndex);
            this.tableName = tableName.substring(periodIndex + 1);
        } else {
            this.schema = dbDialect.getDefaultSchema();
            this.tableName = tableName;
        }

        resetMetaData();
    }

    public void resetMetaData() {
        table = dbDialect.getMetaDataFor(null, schema, tableName, true);
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
        return insert(ctx, columnValues, BinaryEncoding.NONE);
    }

    public int insert(IDataLoaderContext ctx, String[] columnValues, BinaryEncoding encoding) {
        StatementBuilder st = getStatementBuilder(ctx, DmlType.INSERT, encoding);
        return execute(ctx, st, columnValues, columnMetaData, encoding);
    }

    public int update(IDataLoaderContext ctx, String[] columnValues, String[] keyValues) {
        return update(ctx, columnValues, keyValues, BinaryEncoding.NONE);
    }

    public int update(IDataLoaderContext ctx, String[] columnValues, String[] keyValues, BinaryEncoding encoding) {
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
                st = createStatementBuilder(ctx, DmlType.UPDATE, changedColumnNames, encoding);
                columnValues = (String[]) changedColumnValueList.toArray(new String[0]);
                Column[] changedColumnMetaData = (Column[]) changedColumnMetaList.toArray(new Column[0]);
                metaData = (Column[]) ArrayUtils.addAll(changedColumnMetaData, keyMetaData);
            }
            oldData = null;
        } else if (dontIncludeKeysInUpdateStatement) {
            String[] values = removeKeysFromColumnValuesIfSame(ctx, keyValues, columnValues);
            if (values != null) {
                columnValues = values;
                st = getStatementBuilder(ctx, DmlType.UPDATE_NO_KEYS, encoding);
                metaData = noKeyColumnPlusKeyMetaData;
            }
        }

        if (st == null) {
            st = getStatementBuilder(ctx, DmlType.UPDATE, encoding);
            metaData = columnKeyMetaData;
        }
        String[] values = (String[]) ArrayUtils.addAll(columnValues, keyValues);
        return execute(ctx, st, values, metaData, encoding);
    }

    /**
     * This is in support of creating update statements that don't use the keys
     * in the set portion of the update statement.
     * 
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
        StatementBuilder st = getStatementBuilder(ctx, DmlType.DELETE, BinaryEncoding.NONE);
        return execute(ctx, st, keyValues, keyMetaData, BinaryEncoding.NONE);
    }

    private StatementBuilder getStatementBuilder(IDataLoaderContext ctx, DmlType type, BinaryEncoding encoding) {
        StatementBuilder st = statementMap.get(type);
        if (st == null) {
            st = createStatementBuilder(ctx, type, columnNames, encoding);
            statementMap.put(type, st);
        }
        return st;
    }

    private StatementBuilder createStatementBuilder(IDataLoaderContext ctx, DmlType type,
            String[] filteredColumnNames, BinaryEncoding encoding) {
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
        if (table.getSchema() != null && dbDialect.getDefaultSchema() != null
                && !table.getSchema().equals(dbDialect.getDefaultSchema())) {
            tableName = table.getSchema() + "." + tableName;
        }
        if (table.getCatalog() != null && dbDialect.getDefaultCatalog() != null
                && !table.getCatalog().equals(dbDialect.getDefaultCatalog())) {
            tableName = table.getCatalog() + "." + tableName;
        }
        return new StatementBuilder(type, tableName, keyMetaData, getColumnMetaData(filteredColumnNames),
                dbDialect.isBlobOverrideToBinary(), dbDialect.isDateOverrideToTimestamp(), dbDialect
                        .getIdentifierQuoteString());
    }

    private int execute(IDataLoaderContext ctx, StatementBuilder st, String[] values, Column[] metaData,
            BinaryEncoding encoding) {
        List<Object> list = new ArrayList<Object>(values.length);

        for (int i = 0; i < values.length; i++) {
            String value = values[i];
            Object objectValue = value;
            Column column = metaData[i];

            if (column != null) {
                int type = column.getTypeCode();
                if ((value == null || (dbDialect.isEmptyStringNulled() && value.equals(""))) && column.isRequired()
                        && column.isOfTextType()) {
                    objectValue = REQUIRED_FIELD_NULL_SUBSTITUTE;
                }
                if (value != null) {
                    if (type == Types.DATE && ! dbDialect.isDateOverrideToTimestamp()) {
                        objectValue = new Date(getTime(value, TIMESTAMP_PATTERNS));
                    } else if (type == Types.TIMESTAMP
                            || (type == Types.DATE && dbDialect.isDateOverrideToTimestamp())) {
                        objectValue = new Timestamp(getTime(value, TIMESTAMP_PATTERNS));
                    } else if (type == Types.CHAR && dbDialect.isCharSpacePadded()) {
                        objectValue = StringUtils.rightPad(value.toString(), column.getSizeAsInt(), ' ');
                    } else if (type == Types.INTEGER || type == Types.SMALLINT || type == Types.BIT) {
                        objectValue = Integer.valueOf(value);
                    } else if (type == Types.NUMERIC || type == Types.DECIMAL || type == Types.FLOAT || type == Types.DOUBLE) {
                        // The number will have either one period or one comma for the decimal point, but we need a period
                        objectValue = new BigDecimal(value.replace(',', '.'));
                    } else if (type == Types.BOOLEAN) {
                        objectValue = value.equals("1") ? Boolean.TRUE : Boolean.FALSE;
                    } else if (type == Types.BLOB || type == Types.LONGVARBINARY || type == Types.BINARY) {
                        if (encoding == BinaryEncoding.NONE) {
                            objectValue = value.getBytes();
                        } else if (encoding == BinaryEncoding.BASE64) {
                            objectValue = Base64.decodeBase64(value.getBytes());
                        } else if (encoding == BinaryEncoding.HEX) {
                            try {
                                objectValue = Hex.decodeHex(value.toCharArray());
                            } catch (DecoderException e) {
                                throw new RuntimeException(e);
                            }
                        }
                    } else if (type == Types.TIME) {
                        objectValue = new Time(getTime(value, TIMESTAMP_PATTERNS));
                    }
                }
                list.add(objectValue);
            }
        }
        Object[] objectValues = list.toArray();
        if (columnFilters != null) {
            for (IColumnFilter columnFilter : columnFilters) {
                objectValues = columnFilter.filterColumnsValues(ctx, st.getDmlType(), getTable(), objectValues);
            }
        }
        return jdbcTemplate.update(st.getSql(), objectValues, st.getTypes());
    }

    private long getTime(String value, String[] pattern) {
        try {
            return DateUtils.parseDate(value, pattern).getTime();
        } catch (ParseException e) {
            throw new RuntimeException(e);
        }
    }

    public void setKeyNames(String[] keyNames) {
        this.keyNames = keyNames;
        clear();
    }

    public void setColumnNames(String[] columnNames) {
        this.columnNames = columnNames;
        clear();
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

    public void setOldData(String[] oldData) {
        this.oldData = oldData;
    }

    public Table getTable() {
        return table;
    }

}
