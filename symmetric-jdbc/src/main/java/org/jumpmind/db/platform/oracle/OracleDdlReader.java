package org.jumpmind.db.platform.oracle;

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
import java.sql.DatabaseMetaData;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.platform.IDdlBuilder;

/*
 * Reads a database model from an Oracle 8 database.
 */
public class OracleDdlReader extends AbstractJdbcDdlReader {

    /* The regular expression pattern for the Oracle conversion of ISO dates. */
    private Pattern oracleIsoDatePattern;

    /* The regular expression pattern for the Oracle conversion of ISO times. */
    private Pattern oracleIsoTimePattern;

    /*
     * The regular expression pattern for the Oracle conversion of ISO
     * timestamps.
     */
    private Pattern oracleIsoTimestampPattern;

    public OracleDdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
        setDefaultTablePattern("%");

        oracleIsoDatePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD'\\)");
        oracleIsoTimePattern = Pattern.compile("TO_DATE\\('([^']*)'\\, 'HH24:MI:SS'\\)");
        oracleIsoTimestampPattern = Pattern
                .compile("TO_DATE\\('([^']*)'\\, 'YYYY\\-MM\\-DD HH24:MI:SS'\\)");
    }

    @Override
    protected Table readTable(Connection connection, DatabaseMetaDataWrapper metaData,
            Map<String, Object> values) throws SQLException {
        /*
         * Oracle 10 added the recycle bin which contains dropped database
         * objects not yet purged Since we don't want entries from the recycle
         * bin, we filter them out
         */
        boolean tableHasBeenDeleted = isTableInRecycleBin(connection, values);

        /*
         * System tables are in the system schema
         */
        String schema = (String) values.get(getResultSetSchemaName());

        if (!tableHasBeenDeleted && !"SYSTEM".equals(schema)) {

            Table table = super.readTable(connection, metaData, values);
            if (table != null) {
                determineAutoIncrementColumns(connection, table);
            }

            return table;
        } else {
            return null;
        }
    }

    protected boolean isTableInRecycleBin(Connection connection, Map<String, Object> values)
            throws SQLException {
        String tablename = (String)values.get("TABLE_NAME");
        return StringUtils.isNotBlank(tablename) && tablename.toLowerCase().startsWith("bin$");
    }

    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.startsWith("DATE")) {
            return Types.DATE;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")
                && !typeName.endsWith("TIME ZONE")) {
            // This is for Oracle's TIMESTAMP(9)
            return Types.TIMESTAMP;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")
                && typeName.endsWith("WITH TIME ZONE")) {
            return ColumnTypes.ORACLE_TIMESTAMPTZ;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")
                && typeName.endsWith("WITH LOCAL TIME ZONE")) {
            return ColumnTypes.ORACLE_TIMESTAMPLTZ;
        } else if (typeName != null && typeName.startsWith("NVARCHAR")) {
            // This is for Oracle's NVARCHAR type
            return Types.VARCHAR;
        } else if (typeName != null && typeName.startsWith("LONGNVARCHAR")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.startsWith("NCHAR")) {
            return Types.CHAR;
        } else if (typeName != null && typeName.startsWith("XML")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.endsWith("CLOB")) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.startsWith("BINARY_FLOAT")) {
            return Types.FLOAT;
        } else if (typeName != null && typeName.startsWith("BINARY_DOUBLE")) {
            return Types.DOUBLE;
        } else if (typeName != null && typeName.startsWith("BFILE")) {
            return Types.VARCHAR;
        } else if (typeName != null && typeName.startsWith("INTERVAL")) {
            return Types.VARCHAR;            
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        if (column.getMappedTypeCode() == Types.DECIMAL) {
            // We're back-mapping the NUMBER columns returned by Oracle
            // Note that the JDBC driver returns DECIMAL for these NUMBER
            // columns
            if (column.getScale() <= -127 || column.getScale() >= 127) {
                if (column.getSizeAsInt() == 0) {
                    /*
                     * Latest oracle jdbc drivers for 11g return (0,-127) for
                     * types defined as integer resulting in bad mappings.
                     * NUMBER without scale or precision looks the same as
                     * INTEGER in the jdbc driver. We must check the Oracle
                     * meta data to see if type is an INTEGER.
                     */
                    if (isColumnInteger((String)values.get("TABLE_NAME"),
                            (String)values.get("COLUMN_NAME"))) {
                        column.setMappedTypeCode(Types.BIGINT);
                    }
                } else if (column.getSizeAsInt() <= 63) {
                    column.setMappedTypeCode(Types.REAL);
                } else {
                    column.setMappedTypeCode(Types.DOUBLE);
                }
            }
        } else if (column.getMappedTypeCode() == Types.FLOAT) {
            // Same for REAL, FLOAT, DOUBLE PRECISION, which all back-map to
            // FLOAT but with
            // different sizes (63 for REAL, 126 for FLOAT/DOUBLE PRECISION)
            switch (column.getSizeAsInt()) {
                case 63:
                    column.setMappedTypeCode(Types.REAL);
                    break;
                case 126:
                    column.setMappedTypeCode(Types.DOUBLE);
                    break;
            }
        } else if ((column.getMappedTypeCode() == Types.DATE)
                || (column.getMappedTypeCode() == Types.TIMESTAMP)) {
            // we also reverse the ISO-format adaptation, and adjust the default
            // value to timestamp
            if (column.getDefaultValue() != null) {
                Timestamp timestamp = null;

                Matcher matcher = oracleIsoTimestampPattern.matcher(column.getDefaultValue());

                if (matcher.matches()) {
                    String timestampVal = matcher.group(1);
                    timestamp = Timestamp.valueOf(timestampVal);
                } else {
                    matcher = oracleIsoDatePattern.matcher(column.getDefaultValue());
                    if (matcher.matches()) {
                        String dateVal = matcher.group(1);
                        timestamp = new Timestamp(Date.valueOf(dateVal).getTime());
                    } else {
                        matcher = oracleIsoTimePattern.matcher(column.getDefaultValue());
                        if (matcher.matches()) {
                            String timeVal = matcher.group(1);

                            timestamp = new Timestamp(Time.valueOf(timeVal).getTime());
                        }
                    }
                }
                if (timestamp != null) {
                    column.setDefaultValue(timestamp.toString());
                }
            }
        } else if (TypeMap.isTextType(column.getMappedTypeCode())) {
            String defaultValue = column.getDefaultValue();
            if (isNotBlank(defaultValue) && defaultValue.startsWith("('") && defaultValue.endsWith("')")) {
                defaultValue = defaultValue.substring(2, defaultValue.length()-2);
            }
            column.setDefaultValue(unescape(defaultValue, "'", "''"));
        }
        return column;
    }

    private boolean isColumnInteger(String tableName, String columnName) {
        return (platform.getSqlTemplate().queryForInt(
                "select case when data_precision is null and data_scale=0 then 1 else 0 end " +
                "from all_tab_columns where table_name=? and column_name=?", tableName, columnName) == 1);
    }

    /*
     * Helper method that determines the auto increment status using Firebird's
     * system tables.
     *
     * @param table The table
     */
    protected void determineAutoIncrementColumns(Connection connection, Table table)
            throws SQLException {
        Column[] columns = table.getColumns();

        for (int idx = 0; idx < columns.length; idx++) {
            columns[idx].setAutoIncrement(isAutoIncrement(connection, table, columns[idx]));
        }
    }

    /*
     * Tries to determine whether the given column is an identity column.
     *
     * @param table The table
     *
     * @param column The column
     *
     * @return <code>true</code> if the column is an identity column
     */
    protected boolean isAutoIncrement(Connection connection, Table table, Column column)
            throws SQLException {
        // TODO: For now, we only check whether there is a sequence & trigger as
        // generated by DdlUtils
        // But once sequence/trigger support is in place, it might be possible
        // to 'parse' the
        // trigger body (via SELECT trigger_name, trigger_body FROM
        // user_triggers) in order to
        // determine whether it fits our auto-increment definition
        PreparedStatement prepStmt = null;
        IDdlBuilder builder = getPlatform().getDdlBuilder();
        String triggerName = builder.getConstraintName(OracleDdlBuilder.PREFIX_TRIGGER, table,
                column.getName(), null);
        String seqName = builder.getConstraintName(OracleDdlBuilder.PREFIX_SEQUENCE, table,
                column.getName(), null);

        if (!getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn()) {
            triggerName = triggerName.toUpperCase();
            seqName = seqName.toUpperCase();
        }
        try {
            prepStmt = connection
                    .prepareStatement("SELECT * FROM user_triggers WHERE trigger_name = ?");
            prepStmt.setString(1, triggerName);

            ResultSet resultSet = prepStmt.executeQuery();

            if (!resultSet.next()) {
                resultSet.close();
                return false;
            }
            // we have a trigger, so lets check the sequence
            prepStmt.close();

            prepStmt = connection
                    .prepareStatement("SELECT * FROM user_sequences WHERE sequence_name = ?");
            prepStmt.setString(1, seqName);

            resultSet = prepStmt.executeQuery();
            boolean resultFound = resultSet.next();
            resultSet.close();
            return resultFound;
        } finally {
            if (prepStmt != null) {
                prepStmt.close();
            }
        }
    }

    @Override
    protected Collection<IIndex> readIndices(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        // Oracle bug 4999817 causes a table analyze to execute in response to a
        // call to
        // DatabaseMetaData#getIndexInfo.
        // The bug is fixed in driver version 10.2.0.4. The bug is present in at
        // least
        // driver versions 10.2.0.1.0, 10.1.0.2.0, and 9.2.0.5.
        // To avoid this bug, we will access user_indexes view.
        // This also allows us to filter system-generated indices which are
        // identified by either
        // having GENERATED='Y' in the query result, or by their index names
        // being equal to the
        // name of the primary key of the table

        StringBuilder query = new StringBuilder();

        query.append("SELECT a.INDEX_NAME, a.INDEX_TYPE, a.UNIQUENESS, b.COLUMN_NAME, b.COLUMN_POSITION FROM USER_INDEXES a, USER_IND_COLUMNS b WHERE ");
        query.append("a.TABLE_NAME=? AND a.GENERATED=? AND a.TABLE_TYPE=? AND a.TABLE_NAME=b.TABLE_NAME AND a.INDEX_NAME=b.INDEX_NAME AND ");
        query.append("a.INDEX_NAME NOT IN (SELECT DISTINCT c.CONSTRAINT_NAME FROM USER_CONSTRAINTS c WHERE c.CONSTRAINT_TYPE=? AND c.TABLE_NAME=a.TABLE_NAME");
        if (metaData.getSchemaPattern() != null) {
            query.append(" AND c.OWNER LIKE ?) AND a.TABLE_OWNER LIKE ?");
        } else {
            query.append(")");
        }

        Map<String, IIndex> indices = new LinkedHashMap<String, IIndex>();
        PreparedStatement stmt = null;

        try {
            stmt = connection.prepareStatement(query.toString());
            stmt.setString(
                    1,
                    getPlatform().getDdlBuilder().isDelimitedIdentifierModeOn() ? tableName : tableName
                            .toUpperCase());
            stmt.setString(2, "N");
            stmt.setString(3, "TABLE");
            stmt.setString(4, "P");
            if (metaData.getSchemaPattern() != null) {
                stmt.setString(5, metaData.getSchemaPattern().toUpperCase());
                stmt.setString(6, metaData.getSchemaPattern().toUpperCase());
            }

            ResultSet rs = stmt.executeQuery();
            Map<String, Object> values = new HashMap<String, Object>();

            while (rs.next()) {
                String name = rs.getString(1);
                String type = rs.getString(2);
                // Only read in normal oracle indexes
                if (type.startsWith("NORMAL")) {
                    values.put("INDEX_TYPE", new Short(DatabaseMetaData.tableIndexOther));
                    values.put("INDEX_NAME", name);
                    values.put("NON_UNIQUE",
                            "UNIQUE".equalsIgnoreCase(rs.getString(3)) ? Boolean.FALSE
                                    : Boolean.TRUE);
                    values.put("COLUMN_NAME", rs.getString(4));
                    values.put("ORDINAL_POSITION", new Short(rs.getShort(5)));

                    readIndex(metaData, values, indices);
                } else if (log.isDebugEnabled()) {
                    log.debug("Skipping index " + name + " of type " + type);
                }
            }

            rs.close();
        } finally {
            if (stmt != null) {
                stmt.close();
            }
        }
        return indices.values();
    }
    
    @Override
    protected String getTableNamePattern(String tableName) {
        /*
         * When looking up a table definition, Oracle treats underscore (_) in
         * the table name as a wildcard, so it needs to be escaped, or you'll
         * get back column names for more than one table. Example:
         * DatabaseMetaData.metaData.getColumns(null, null, "SYM\\_NODE", null)
         */
       return String.format("%s", tableName).replaceAll("\\_", "/_");
       //return tableName;
    }
}
