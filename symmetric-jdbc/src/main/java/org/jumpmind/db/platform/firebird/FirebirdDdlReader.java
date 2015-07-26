package org.jumpmind.db.platform.firebird;

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
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.collections.map.ListOrderedMap;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;
import org.jumpmind.db.sql.JdbcSqlTemplate;

/*
 * The Jdbc Model Reader for Firebird.
 */
public class FirebirdDdlReader extends AbstractJdbcDdlReader {

    public FirebirdDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");
    }
    
    @Override
    protected Collection<String> readPrimaryKeyNames(DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        return super.readPrimaryKeyNames(metaData, getTableNamePattern(tableName));
    } 
    
    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map<String,Object> values)
            throws SQLException {
        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            determineAutoIncrementColumns(connection, table);
            setPrimaryKeyConstraintName(connection, table);
        }

        return table;
    }
    
    protected void setPrimaryKeyConstraintName(Connection connection, Table table) throws SQLException {
        String sql = "select RDB$CONSTRAINT_NAME from RDB$RELATION_CONSTRAINTS where RDB$RELATION_NAME=? and RDB$CONSTRAINT_TYPE='PRIMARY KEY'";
        PreparedStatement pstmt = null;
        try {
            pstmt = connection.prepareStatement(sql);
            pstmt.setString(1, table.getName()); 
            ResultSet rs = pstmt.executeQuery();
            if (rs.next()) {
                table.setPrimaryKeyConstraintName(rs.getString(1).trim());
            }
            rs.close();
        } finally {
            JdbcSqlTemplate.close(pstmt);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String,Object> values) throws SQLException {
        Column column = super.readColumn(metaData, values);

        if (column.getMappedTypeCode() == Types.FLOAT) {
            column.setMappedTypeCode(Types.REAL);
        } else if (TypeMap.isTextType(column.getMappedTypeCode())) {
            column.setDefaultValue(unescape(column.getDefaultValue(), "'", "''"));
        }
        return column;
    }

    /*
     * Helper method that determines the auto increment status using Firebird's
     * system tables.
     * 
     * @param table The table
     */
    protected void determineAutoIncrementColumns(Connection connection, Table table)
            throws SQLException {
        /*
         * Since for long table and column names, the trigger name will be
         * shortened we have to determine for each column whether there is a
         * trigger on it
         */
        Column[] columns = table.getColumns();
        HashMap<String, Column> names = new HashMap<String, Column>();
        String name;

        for (int idx = 0; idx < columns.length; idx++) {
            name = ((FirebirdDdlBuilder) getPlatform().getDdlBuilder()).getTriggerName(table,
                    columns[idx]);
            if (!getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                name = name.toUpperCase();
            }
            names.put(name, columns[idx]);
        }

        PreparedStatement stmt = connection.prepareStatement("SELECT RDB$TRIGGER_NAME FROM RDB$TRIGGERS WHERE RDB$RELATION_NAME=?");
        stmt.setString(1, table.getName());
        ResultSet rs = null;
        try {
            rs = stmt.executeQuery();
            while (rs.next()) {
                String triggerName = rs.getString(1).trim();
                Column column = (Column) names.get(triggerName);
                if (column != null) {
                    column.setAutoIncrement(true);
                }
            }
            
        } finally {
            if (rs != null) {
                rs.close();
            }
            stmt.close();
        }
    }

    @Override
    protected Collection<IIndex> readIndices(Connection connection, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        tableName = getTableNamePattern(tableName);
        // Jaybird is not able to read indices when delimited identifiers are
        // turned on, so we gather the data manually using Firebird's system tables
        @SuppressWarnings("unchecked")
        Map<String, IIndex> indices = new ListOrderedMap();
        StringBuilder query = new StringBuilder();

        query.append("SELECT a.RDB$INDEX_NAME INDEX_NAME, b.RDB$RELATION_NAME TABLE_NAME, b.RDB$UNIQUE_FLAG NON_UNIQUE,");
        query.append(" a.RDB$FIELD_POSITION ORDINAL_POSITION, a.RDB$FIELD_NAME COLUMN_NAME, 3 INDEX_TYPE");
        query.append(" FROM RDB$INDEX_SEGMENTS a, RDB$INDICES b WHERE a.RDB$INDEX_NAME=b.RDB$INDEX_NAME AND b.RDB$RELATION_NAME = ?");

        PreparedStatement stmt = connection.prepareStatement(query.toString());
        ResultSet indexData = null;

        stmt.setString(1,
                getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? tableName : tableName.toUpperCase());

        try {
            indexData = stmt.executeQuery();

            while (indexData.next()) {
                Map<String,Object> values = readMetaData(indexData, getColumnsForIndex());

                // we have to reverse the meaning of the unique flag
                values.put("NON_UNIQUE",
                        Boolean.FALSE.equals(values.get("NON_UNIQUE")) ? Boolean.TRUE
                                : Boolean.FALSE);
                // and trim the names
                values.put("INDEX_NAME", ((String) values.get("INDEX_NAME")).trim());
                values.put("TABLE_NAME", ((String) values.get("TABLE_NAME")).trim());
                values.put("COLUMN_NAME", ((String) values.get("COLUMN_NAME")).trim());
                readIndex(metaData, values, indices);
            }
        } finally {
            if (indexData != null) {
                indexData.close();
            }
        }
        return indices.values();
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        IDdlBuilder builder = getPlatform().getDdlBuilder();
        String tableName = builder.getTableName(table.getName());
        String indexName = builder.getIndexName(index);
        StringBuilder query = new StringBuilder();

        query.append("SELECT RDB$CONSTRAINT_NAME FROM RDB$RELATION_CONSTRAINTS where RDB$RELATION_NAME=? AND RDB$CONSTRAINT_TYPE=? AND RDB$INDEX_NAME=?");

        PreparedStatement stmt = connection.prepareStatement(query.toString());

        try {
            stmt.setString(
                    1,
                    getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? tableName : tableName
                            .toUpperCase());
            stmt.setString(2, "PRIMARY KEY");
            stmt.setString(3, indexName);

            ResultSet resultSet = stmt.executeQuery();

            return resultSet.next();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    @Override
    protected boolean isInternalForeignKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, ForeignKey fk, IIndex index)
            throws SQLException {
        IDdlBuilder builder = getPlatform().getDdlBuilder();
        String tableName = builder.getTableName(table.getName());
        String indexName = builder.getIndexName(index);
        String fkName = builder.getForeignKeyName(table, fk);
        StringBuilder query = new StringBuilder();

        query.append("SELECT RDB$CONSTRAINT_NAME FROM RDB$RELATION_CONSTRAINTS where RDB$RELATION_NAME=? AND RDB$CONSTRAINT_TYPE=? AND RDB$CONSTRAINT_NAME=? AND RDB$INDEX_NAME=?");

        PreparedStatement stmt = connection.prepareStatement(query.toString());

        try {
            stmt.setString(
                    1,
                    getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? tableName : tableName
                            .toUpperCase());
            stmt.setString(2, "FOREIGN KEY");
            stmt.setString(3, fkName);
            stmt.setString(4, indexName);

            ResultSet resultSet = stmt.executeQuery();

            return resultSet.next();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
    }

    @Override
    protected String getTableNamePattern(String tableName) {
        /*
         * When looking up a table definition, Jaybird treats underscore (_) in
         * the table name as a wildcard, so it needs to be escaped, or you'll
         * get back column names for more than one table. Example:
         * DatabaseMetaData.metaData.getColumns(null, null, "SYM\\_NODE", null)
         */
        return String.format("\"%s\"", tableName).replaceAll("\\_", "\\\\_");
    }

}
