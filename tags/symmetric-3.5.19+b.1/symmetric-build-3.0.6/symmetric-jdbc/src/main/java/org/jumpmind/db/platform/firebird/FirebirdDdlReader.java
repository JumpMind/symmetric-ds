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
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
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
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData, Map<String,Object> values)
            throws SQLException {
        Table table = super.readTable(connection, metaData, values);

        if (table != null) {
            determineAutoIncrementColumns(connection, table);
        }

        return table;
    }

    @Override
    protected Collection<Column> readColumns(DatabaseMetaDataWrapper metaData, String tableName)
            throws SQLException {
        ResultSet columnData = null;
        try {
            List<Column> columns = new ArrayList<Column>();

            if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                // Jaybird has a problem when delimited identifiers are used as
                // it is not able to find the columns for the table
                // So we have to filter manually below
                columnData = metaData.getColumns(getDefaultTablePattern(),
                        getDefaultColumnPattern());

                while (columnData.next()) {
                    Map<String,Object> values = readMetaData(columnData, getColumnsForColumn());

                    if (tableName.equals(values.get("TABLE_NAME"))) {
                        columns.add(readColumn(metaData, values));
                    }
                }
            } else {
                columnData = metaData.getColumns(tableName, getDefaultColumnPattern());

                while (columnData.next()) {
                    Map<String,Object> values = readMetaData(columnData, getColumnsForColumn());

                    if (tableName.equals(values.get("TABLE_NAME"))) {
                        columns.add(readColumn(metaData, values));
                    }
                }
            }

            return columns;
        } finally {
            if (columnData != null) {
                columnData.close();
            }
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
        // Since for long table and column names, the generator name will be
        // shortened
        // we have to determine for each column whether there is a generator for
        // it
        Column[] columns = table.getColumns();
        HashMap<String, Column> names = new HashMap<String, Column>();
        String name;

        for (int idx = 0; idx < columns.length; idx++) {
            name = ((FirebirdDdlBuilder) getPlatform().getDdlBuilder()).getGeneratorName(table,
                    columns[idx]);
            if (!getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                name = name.toUpperCase();
            }
            names.put(name, columns[idx]);
        }

        Statement stmt = connection.createStatement();

        try {
            ResultSet rs = stmt.executeQuery("SELECT RDB$GENERATOR_NAME FROM RDB$GENERATORS");

            while (rs.next()) {
                String generatorName = rs.getString(1).trim();
                Column column = (Column) names.get(generatorName);

                if (column != null) {
                    column.setAutoIncrement(true);
                }
            }
            rs.close();
        } finally {
            stmt.close();
        }
    }

    @Override
    protected Collection<String> readPrimaryKeyNames(DatabaseMetaDataWrapper metaData, String tableName)
            throws SQLException {
        List<String> pks = new ArrayList<String>();
        ResultSet pkData = null;

        try {
            if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                // Jaybird has a problem when delimited identifiers are used as
                // it is not able to find the primary key info for the table
                // So we have to filter manually below
                pkData = metaData.getPrimaryKeys(getDefaultTablePattern());
                while (pkData.next()) {
                    Map<String,Object> values = readMetaData(pkData, getColumnsForPK());

                    if (tableName.equals(values.get("TABLE_NAME"))) {
                        pks.add(readPrimaryKeyName(metaData, values));
                    }
                }
            } else {
                pkData = metaData.getPrimaryKeys(tableName);
                while (pkData.next()) {
                    Map<String,Object> values = readMetaData(pkData, getColumnsForPK());

                    if (tableName.equals(values.get("TABLE_NAME"))) {
                        pks.add(readPrimaryKeyName(metaData, values));
                    }
                }
            }
        } finally {
            if (pkData != null) {
                pkData.close();
            }
        }
        return pks;
    }

    @Override
    protected Collection<ForeignKey> readForeignKeys(Connection connection, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        @SuppressWarnings("unchecked")
        Map<String, ForeignKey> fks = new ListOrderedMap();
        ResultSet fkData = null;

        try {
            if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                // Jaybird has a problem when delimited identifiers are used as
                // it is not able to find the foreign key info for the table
                // So we have to filter manually below
                fkData = metaData.getForeignKeys(getDefaultTablePattern());
                while (fkData.next()) {
                    Map<String,Object> values = readMetaData(fkData, getColumnsForFK());

                    if (tableName.equals(values.get("FKTABLE_NAME"))) {
                        readForeignKey(metaData, values, fks);
                    }
                }
            } else {
                fkData = metaData.getForeignKeys(tableName);
                while (fkData.next()) {
                    Map<String,Object> values = readMetaData(fkData, getColumnsForFK());

                    if (tableName.equals(values.get("FKTABLE_NAME"))) {
                        readForeignKey(metaData, values, fks);
                    }
                }
            }
        } finally {
            if (fkData != null) {
                fkData.close();
            }
        }
        return fks.values();
    }

    @Override
    protected Collection<IIndex> readIndices(Connection connection, DatabaseMetaDataWrapper metaData,
            String tableName) throws SQLException {
        // Jaybird is not able to read indices when delimited identifiers are
        // turned on,
        // so we gather the data manually using Firebird's system tables
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
    public String determineSchemaOf(Connection connection, String schemaPattern, Table table)
            throws SQLException {
        ResultSet tableData = null;
        ResultSet columnData = null;

        try {
            DatabaseMetaDataWrapper metaData = new DatabaseMetaDataWrapper();

            metaData.setMetaData(connection.getMetaData());
            metaData.setCatalog(getDefaultCatalogPattern());
            metaData.setSchemaPattern(schemaPattern == null ? getDefaultSchemaPattern()
                    : schemaPattern);
            metaData.setTableTypes(getDefaultTableTypes());

            String tablePattern = table.getName();

            if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                tablePattern = tablePattern.toUpperCase();
            }

            tableData = metaData.getTables(tablePattern);

            boolean found = false;
            String schema = null;

            while (!found && tableData.next()) {
                Map<String,Object> values = readMetaData(tableData, getColumnsForTable());
                String tableName = (String) values.get("TABLE_NAME");

                if ((tableName != null) && (tableName.length() > 0)) {
                    schema = (String) values.get("TABLE_SCHEM");
                    found = true;

                    if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
                        // Jaybird has a problem when delimited identifiers are
                        // used as
                        // it is not able to find the columns for the table
                        // So we have to filter manually below
                        columnData = metaData.getColumns(getDefaultTablePattern(),
                                getDefaultColumnPattern());
                    } else {
                        columnData = metaData.getColumns(tableName, getDefaultColumnPattern());
                    }

                    while (found && columnData.next()) {
                        values = readMetaData(columnData, getColumnsForColumn());

                        if (getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()
                                && !tableName.equals(values.get("TABLE_NAME"))) {
                            continue;
                        }

                        if (table.findColumn((String) values.get("COLUMN_NAME"), getPlatform().getDdlBuilder()
                                .isDelimitedIdentifierModeOn()) == null) {
                            found = false;
                        }
                    }
                    columnData.close();
                    columnData = null;
                }
            }
            return found ? schema : null;
        } finally {
            if (columnData != null) {
                columnData.close();
            }
            if (tableData != null) {
                tableData.close();
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
