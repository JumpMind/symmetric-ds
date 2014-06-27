package org.jumpmind.db.platform.postgresql;

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

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTemplate;

/*
 * Reads a database model from a PostgreSql database.
 */
public class PostgreSqlDdlReader extends AbstractJdbcDdlReader {

    public PostgreSqlDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern(null);
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            // PostgreSQL also returns unique indices for non-pk auto-increment
            // columns which are of the form "[table]_[column]_key"
            HashMap<String,IIndex> uniquesByName = new HashMap<String,IIndex>();

            for (int indexIdx = 0; indexIdx < table.getIndexCount(); indexIdx++) {
                IIndex index = table.getIndex(indexIdx);

                if (index.isUnique() && (index.getName() != null)) {
                    uniquesByName.put(index.getName(), index);
                }
            }
            for (int columnIdx = 0; columnIdx < table.getColumnCount(); columnIdx++) {
                Column column = table.getColumn(columnIdx);
                if (column.isAutoIncrement() && !column.isPrimaryKey()) {
                    String indexName = table.getName() + "_" + column.getName() + "_key";
                    if (uniquesByName.containsKey(indexName)) {
                        table.removeIndex((IIndex) uniquesByName.get(indexName));
                        uniquesByName.remove(indexName);
                    }
                }
            }
        }
        setPrimaryKeyConstraintName(connection, table);
        return table;
    }
    
    protected void setPrimaryKeyConstraintName(Connection connection, Table table) throws SQLException {
        String sql = "select conname from pg_constraint where conrelid in (select oid from pg_class where relname=? and relnamespace in (select oid from pg_namespace where nspname=?)) and contype='p'";
        PreparedStatement pstmt = null;
        ResultSet rs = null;
        try {
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, table.getName());
            pstmt.setString(2, table.getSchema());
            rs = pstmt.executeQuery();
            if (rs.next()) {
                table.setPrimaryKeyConstraintName(rs.getString(1).trim());
            }            
        } finally {
            JdbcSqlTemplate.close(rs);
            JdbcSqlTemplate.close(pstmt);
        }
    }

    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.equalsIgnoreCase("ABSTIME")) {
            return Types.TIMESTAMP;
        } else if (typeName != null && typeName.equalsIgnoreCase("TIMESTAMPTZ")) {
            // lets use the same type code that oracle uses
            return -101;            
        } else if (PostgreSqlDatabasePlatform.isBlobStoredByReference(typeName)) {
            return Types.BLOB;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException {
        Column column = super.readColumn(metaData, values);

        if (column.getSize() != null) {
            if (column.getSizeAsInt() <= 0) {
                column.setSize(null);
                // PostgreSQL reports BYTEA and TEXT as BINARY(-1) and
                // VARCHAR(-1) respectively
                // Since we cannot currently use the Blob/Clob interface with
                // BYTEA, we instead
                // map them to LONGVARBINARY/LONGVARCHAR
                if (column.getMappedTypeCode() == Types.BINARY) {
                    column.setMappedTypeCode(Types.LONGVARBINARY);
                } else if (column.getMappedTypeCode() == Types.VARCHAR) {
                    column.setMappedTypeCode(Types.LONGVARCHAR);
                }
            }
            // fix issue DDLUTILS-165 as postgresql-8.2-504-jdbc3.jar seems to
            // return Integer.MAX_VALUE
            // on columns defined as TEXT.
            else if (column.getSizeAsInt() == Integer.MAX_VALUE) {
                column.setSize(null);
                if (column.getMappedTypeCode() == Types.VARCHAR) {
                    column.setMappedTypeCode(Types.LONGVARCHAR);
                } else if (column.getMappedTypeCode() == Types.BINARY) {
                    column.setMappedTypeCode(Types.LONGVARBINARY);
                }
            }
        }

        String defaultValue = column.getDefaultValue();

        if ((defaultValue != null) && (defaultValue.length() > 0)) {
            // If the default value looks like
            // "nextval('ROUNDTRIP_VALUE_seq'::text)"
            // then it is an auto-increment column
            if (defaultValue.startsWith("nextval(") || 
                    (PostgreSqlDdlBuilder.isUsePseudoSequence() && defaultValue.endsWith("seq()"))) {
                column.setAutoIncrement(true);
                defaultValue = null;
            } else {
                // PostgreSQL returns default values in the forms
                // "-9000000000000000000::bigint" or
                // "'some value'::character varying" or "'2000-01-01'::date"
                switch (column.getMappedTypeCode()) {
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.DECIMAL:
                case Types.NUMERIC:
                    defaultValue = extractUndelimitedDefaultValue(defaultValue);
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                case Types.DATE:
                case Types.TIME:
                case Types.TIMESTAMP:
                    defaultValue = extractDelimitedDefaultValue(defaultValue);
                    break;
                }
                if (TypeMap.isTextType(column.getMappedTypeCode())) {
                    // We assume escaping via double quote (see also the
                    // backslash_quote setting:
                    // http://www.postgresql.org/docs/7.4/interactive/runtime-config.html#RUNTIME-CONFIG-COMPATIBLE)
                    defaultValue = unescape(defaultValue, "'", "''");
                }
            }
            column.setDefaultValue(defaultValue);
        }
        return column;
    }

    /*
     * Extractes the default value from a default value spec of the form
     * "'some value'::character varying" or "'2000-01-01'::date".
     * 
     * @param defaultValue The default value spec
     * 
     * @return The default value
     */
    private String extractDelimitedDefaultValue(String defaultValue) {
        if (defaultValue.startsWith("'")) {
            int valueEnd = defaultValue.indexOf("'::");

            if (valueEnd > 0) {
                return defaultValue.substring("'".length(), valueEnd);
            }
        }
        return defaultValue;
    }

    /*
     * Extractes the default value from a default value spec of the form
     * "-9000000000000000000::bigint".
     * 
     * @param defaultValue The default value spec
     * 
     * @return The default value
     */
    private String extractUndelimitedDefaultValue(String defaultValue) {
        int valueEnd = defaultValue.indexOf("::");

        if (valueEnd > 0) {
            defaultValue = defaultValue.substring(0, valueEnd);
        } else {
            if (defaultValue.startsWith("(") && defaultValue.endsWith(")")) {
                defaultValue = defaultValue.substring(1, defaultValue.length() - 1);
            }
        }
        return defaultValue;
    }

    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index) {
        // PostgreSQL does not return an index for a foreign key
        return false;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        return table.doesIndexContainOnlyPrimaryKeyColumns(index);
    }

}
