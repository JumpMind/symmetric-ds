package org.jumpmind.db.platform.mssql;

/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.DdlException;
import org.jumpmind.db.platform.IDatabasePlatform;

/*
 * Reads a database model from a Microsoft Sql Server database.
 */
public class MsSqlDdlReader extends AbstractJdbcDdlReader {

    /* Known system tables that Sql Server creates (e.g. automatic maintenance). */
    private static final String[] KNOWN_SYSTEM_TABLES = { "dtproperties" };

    /* The regular expression pattern for the ISO dates. */
    private Pattern isoDatePattern = Pattern.compile("'(\\d{4}\\-\\d{2}\\-\\d{2})'");

    /* The regular expression pattern for the ISO times. */
    private Pattern isoTimePattern = Pattern.compile("'(\\d{2}:\\d{2}:\\d{2})'");

    public MsSqlDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");
    }

    @Override
    protected String getTableNamePattern(String tableName) {
        tableName = tableName.replace("_", "\\_");
        tableName = tableName.replace("%", "\\%");
        return tableName;
    }
    
    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        String tableName = (String) values.get("TABLE_NAME");

        for (int idx = 0; idx < KNOWN_SYSTEM_TABLES.length; idx++) {
            if (KNOWN_SYSTEM_TABLES[idx].equals(tableName)) {
                return null;
            }
        }

        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            // Sql Server does not return the auto-increment status via the
            // database metadata
            determineAutoIncrementFromResultSetMetaData(connection, table, table.getColumns());

            // TODO: Replace this manual filtering using named pks once they are
            // available
            // This is then probably of interest to every platform
            for (int idx = 0; idx < table.getIndexCount();) {
                IIndex index = table.getIndex(idx);

                if (index.isUnique() && existsPKWithName(metaData, table, index.getName())) {
                    table.removeIndex(idx);
                } else {
                    idx++;
                }
            }
        }
        return table; 
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        // Sql Server generates an index "PK__[table name]__[hex number]"
        StringBuffer pkIndexName = new StringBuffer();

        pkIndexName.append("PK__");
        pkIndexName.append(table.getName());
        pkIndexName.append("__");

        return index.getName().toUpperCase().startsWith(pkIndexName.toString().toUpperCase());
    }

    /*
     * Determines whether there is a pk for the table with the given name.
     * 
     * @param metaData The database metadata
     * 
     * @param table The table
     * 
     * @param name The pk name
     * 
     * @return <code>true</code> if there is such a pk
     */
    private boolean existsPKWithName(DatabaseMetaDataWrapper metaData, Table table, String name) {
        try {
            ResultSet pks = metaData.getPrimaryKeys(table.getName());
            boolean found = false;

            while (pks.next() && !found) {
                if (name.equals(pks.getString("PK_NAME"))) {
                    found = true;
                }
            }
            pks.close();
            return found;
        } catch (SQLException ex) {
            throw new DdlException(ex);
        }
    }

    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        int size = -1;
        String columnSize = (String) values.get("COLUMN_SIZE");
            if (isNotBlank(columnSize)) {
                size = Integer.parseInt(columnSize);
            }
        if (typeName != null && typeName.toLowerCase().startsWith("text")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.toLowerCase().startsWith("ntext")) {
            return Types.CLOB;
        } else if (typeName != null && typeName.toLowerCase().equals("float")) {
            return Types.FLOAT;
        } else if (typeName != null && typeName.toUpperCase().contains(TypeMap.GEOMETRY)) {
            return Types.VARCHAR;
        } else if (typeName != null && typeName.toUpperCase().contains("VARCHAR") && size > 8000) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.toUpperCase().contains("NVARCHAR") && size > 4000) {
            return Types.LONGNVARCHAR;
        } else if (typeName != null && typeName.toUpperCase().equals("SQL_VARIANT")) {
            return Types.BINARY;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        String defaultValue = column.getDefaultValue();

        // Sql Server tends to surround the returned default value with one or
        // two sets of parentheses
        if (defaultValue != null) {
            while (defaultValue.startsWith("(") && defaultValue.endsWith(")")) {
                defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
            }

            if (column.getMappedTypeCode() == Types.TIMESTAMP) {
                // Sql Server maintains the default values for DATE/TIME jdbc
                // types, so we have to
                // migrate the default value to TIMESTAMP
                Matcher matcher = isoDatePattern.matcher(defaultValue);
                Timestamp timestamp = null;

                if (matcher.matches()) {
                    timestamp = new Timestamp(Date.valueOf(matcher.group(1)).getTime());
                } else {
                    matcher = isoTimePattern.matcher(defaultValue);
                    if (matcher.matches()) {
                        timestamp = new Timestamp(Time.valueOf(matcher.group(1)).getTime());
                    }
                }
                if (timestamp != null) {
                    defaultValue = timestamp.toString();
                }
            } else if (column.getMappedTypeCode() == Types.DECIMAL || 
            		column.getMappedTypeCode() == Types.BIGINT) {
                // For some reason, Sql Server 2005 always returns DECIMAL
                // default values with a dot
                // even if the scale is 0, so we remove the dot
                if ((column.getScale() == 0) && defaultValue.endsWith(".")) {
                    defaultValue = defaultValue.substring(0, defaultValue.length() - 1);
                }
            } else if (TypeMap.isTextType(column.getMappedTypeCode())) {
                if (defaultValue.startsWith("N'") && defaultValue.endsWith("'")) {
                    defaultValue = defaultValue.substring(2, defaultValue.length()-1);
                }
                defaultValue = unescape(defaultValue, "'", "''");
            }

            column.setDefaultValue(defaultValue);
        }
        
        if ((column.getMappedTypeCode() == Types.DECIMAL) && (column.getSizeAsInt() == 19)
                && (column.getScale() == 0)) {
            column.setMappedTypeCode(Types.BIGINT);
        }
        
        // These columns return sizes and/or decimal places with the metat data from MSSql Server however
        // the values are not adjustable through the create table so they are omitted 
        if (column.getJdbcTypeName() != null && 
                (column.getJdbcTypeName().equals("smallmoney") 
                || column.getJdbcTypeName().equals("money") 
                || column.getJdbcTypeName().equals("timestamp")
                || column.getJdbcTypeName().equals("uniqueidentifier")
                || column.getJdbcTypeName().equals("time")
                || column.getJdbcTypeName().equals("datetime2")
                || column.getJdbcTypeName().equals("date"))) {
            removePlatformSizeAndDecimal(column);
        }
        return column;
    }
    
    protected void removePlatformSizeAndDecimal(Column column) {
        for (PlatformColumn platformColumn : column.getPlatformColumns().values()) {
            platformColumn.setSize(-1);
            platformColumn.setDecimalDigits(-1);
        }
    }
}
