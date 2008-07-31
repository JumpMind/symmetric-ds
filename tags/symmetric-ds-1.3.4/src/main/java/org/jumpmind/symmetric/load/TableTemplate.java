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
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.codec.binary.Base64;
import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
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
    
    private Map<String, Column> allMetaData;

    private Column[] keyMetaData;

    private Column[] columnMetaData;
    
    private Column[] columnKeyMetaData;
    
    private HashMap<DmlType, StatementBuilder> statementMap;

    private List<IColumnFilter> columnFilters = new ArrayList<IColumnFilter>();

    public TableTemplate(JdbcTemplate jdbcTemplate, IDbDialect dbDialect, String tableName, IColumnFilter columnFilter) {
        this.jdbcTemplate = jdbcTemplate;
        this.dbDialect = dbDialect;
        this.setupColumnFilters(columnFilter, dbDialect);
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

    public int insert(String[] columnValues) {
        return insert(columnValues, BinaryEncoding.NONE);
    }

    public int insert(String[] columnValues, BinaryEncoding encoding) {
        StatementBuilder st = getStatementBuilder(DmlType.INSERT, encoding);
        return execute(st, columnValues, columnMetaData, encoding);
    }

    public int update(String[] columnValues, String[] keyValues) {
        return update(columnValues, keyValues, BinaryEncoding.NONE);
    }

    public int update(String[] columnValues, String[] keyValues, BinaryEncoding encoding) {
        StatementBuilder st = getStatementBuilder(DmlType.UPDATE, encoding);
        String[] values = (String[]) ArrayUtils.addAll(columnValues, keyValues);
        return execute(st, values, columnKeyMetaData, encoding);
    }

    public int delete(String[] keyValues) {
        StatementBuilder st = getStatementBuilder(DmlType.DELETE, BinaryEncoding.NONE);
        return execute(st, keyValues, keyMetaData, BinaryEncoding.NONE);
    }

    private StatementBuilder getStatementBuilder(DmlType type, BinaryEncoding encoding) {
        StatementBuilder st = statementMap.get(type);
        if (st == null) {
            String[] filteredColumnNames = columnNames;
            if (columnFilters != null) {
                for (IColumnFilter columnFilter : columnFilters) {
                    filteredColumnNames = columnFilter.filterColumnsNames(type, getTable(), columnNames);
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

            st = new StatementBuilder(type, tableName, keyMetaData,
                    getColumnMetaData(filteredColumnNames), dbDialect.isBlobOverrideToBinary());
            statementMap.put(type, st);
        }
        return st;
    }

    private int execute(StatementBuilder st, String[] values, Column[] metaData, BinaryEncoding encoding) {
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
                    if (type == Types.DATE || type == Types.TIMESTAMP) {
                        objectValue = new Timestamp(getTime(value, TIMESTAMP_PATTERNS));
                    } else if (type == Types.CHAR && dbDialect.isCharSpacePadded()) {
                        objectValue = StringUtils.rightPad(value.toString(), column.getSizeAsInt(), ' ');
                    } else if (type == Types.INTEGER || type == Types.SMALLINT) {
                        objectValue = Integer.valueOf(value);
                    } else if (type == Types.NUMERIC || type == Types.DECIMAL) {
                        objectValue = new BigDecimal(value);
                    } else if (type == Types.BLOB || type == Types.LONGVARBINARY || type == Types.BINARY) {
                        if (encoding == BinaryEncoding.NONE) {
                            objectValue = value.getBytes();
                        } else if (encoding == BinaryEncoding.BASE64) {
                            objectValue = Base64.decodeBase64(value.getBytes());
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
                objectValues = columnFilter.filterColumnsValues(st.getDmlType(), getTable(), objectValues);    
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

}
