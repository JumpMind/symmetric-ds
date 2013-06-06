package org.jumpmind.db.platform.db2;

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
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.JdbcSqlTemplate;

/*
 * Reads a database model from a Db2 UDB database.
 */
public class Db2DdlReader extends AbstractJdbcDdlReader {
    /* Known system tables that Db2 creates (e.g. automatic maintenance). */
    private static final String[] KNOWN_SYSTEM_TABLES = { "STMG_DBSIZE_INFO", "HMON_ATM_INFO",
            "HMON_COLLECTION", "POLICY" };

    /* The regular expression pattern for the time values that Db2 returns. */
    private Pattern db2TimePattern = Pattern.compile("'(\\d{2}).(\\d{2}).(\\d{2})'");

    /* The regular expression pattern for the timestamp values that Db2 returns. */
    private Pattern db2TimestampPattern = Pattern
            .compile("'(\\d{4}\\-\\d{2}\\-\\d{2})\\-(\\d{2}).(\\d{2}).(\\d{2})(\\.\\d{1,8})?'");

    public Db2DdlReader(IDatabasePlatform platform) {
        super(platform);
        setDefaultCatalogPattern(null);
        setDefaultSchemaPattern(null);
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
            // DB2 does not return the auto-increment status via the database
            // metadata
            String sql = "SELECT COLNAME FROM SYSCAT.COLUMNS WHERE TABNAME=? AND IDENTITY=?";
            if (StringUtils.isNotBlank(metaData.getSchemaPattern())) {
                sql = sql + " AND TABSCHEMA=?";
            }

            PreparedStatement pstmt = null;
            ResultSet rs = null;
            try {
                pstmt = connection.prepareStatement(sql);
                pstmt.setString(1, table.getName());
                pstmt.setString(2, "Y");
                if (StringUtils.isNotBlank(metaData.getSchemaPattern())) {
                    pstmt.setString(3, metaData.getSchemaPattern());
                }

                rs = pstmt.executeQuery();
                while (rs.next()) {
                    String columnName = rs.getString(1);
                    Column column = table.getColumnWithName(columnName);
                    if (column != null) {
                        column.setAutoIncrement(true);
                    }
                    if (log.isDebugEnabled()) {
                        log.debug("Found identity column {} on {}", columnName, table.getName());
                    }
                }
            } finally {
                JdbcSqlTemplate.close(rs);
                JdbcSqlTemplate.close(pstmt);
            }
        }
        return table;
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);

        if (column.getDefaultValue() != null) {
            if (column.getMappedTypeCode() == Types.TIME) {
                Matcher matcher = db2TimePattern.matcher(column.getDefaultValue());

                // Db2 returns "HH24.MI.SS"
                if (matcher.matches()) {
                    StringBuffer newDefault = new StringBuffer();

                    newDefault.append("'");
                    // the hour
                    newDefault.append(matcher.group(1));
                    newDefault.append(":");
                    // the minute
                    newDefault.append(matcher.group(2));
                    newDefault.append(":");
                    // the second
                    newDefault.append(matcher.group(3));
                    newDefault.append("'");

                    column.setDefaultValue(newDefault.toString());
                }
            } else if (column.getMappedTypeCode() == Types.TIMESTAMP) {

                Matcher matcher = db2TimestampPattern.matcher(column.getDefaultValue());

                // Db2 returns "YYYY-MM-DD-HH24.MI.SS.FF"
                if (matcher.matches()) {
                    StringBuffer newDefault = new StringBuffer();

                    newDefault.append("'");
                    // group 1 is the date which has the correct format
                    newDefault.append(matcher.group(1));
                    newDefault.append(" ");
                    // the hour
                    newDefault.append(matcher.group(2));
                    newDefault.append(":");
                    // the minute
                    newDefault.append(matcher.group(3));
                    newDefault.append(":");
                    // the second
                    newDefault.append(matcher.group(4));
                    // optionally, the fraction
                    if ((matcher.groupCount() > 4) && (matcher.group(4) != null)) {
                        newDefault.append(matcher.group(5));
                    }
                    newDefault.append("'");

                    column.setDefaultValue(newDefault.toString());
                }
            } else if (TypeMap.isTextType(column.getMappedTypeCode())) {
                String defaultValue = column.getDefaultValue();            
                // DB2 stores default text values quoted.  Remove the quotes
                if ((defaultValue.length() >= 2) && defaultValue.startsWith("'") && defaultValue.endsWith("'")) {
                    defaultValue = defaultValue.substring(1, defaultValue.length()-1);
                }
                column.setDefaultValue(unescape(defaultValue, "'", "''"));
            }
        }
        return column;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) throws SQLException {
        // Db2 uses the form "SQL060205225246220" if the primary key was defined
        // during table creation
        // When the ALTER TABLE way was used however, the index has the name of
        // the primary key
        if (index.getName().startsWith("SQL")) {
            try {
                Long.parseLong(index.getName().substring(3));
                return true;
            } catch (NumberFormatException ex) {
                // we ignore it
            }
            return false;
        } else {
            // we'll compare the index name to the names of all primary keys
            // TODO: Once primary key names are supported, this can be done
            // easier via the table object
            ResultSet pkData = null;
            HashSet<String> pkNames = new HashSet<String>();

            try {
                pkData = metaData.getPrimaryKeys(table.getName());
                while (pkData.next()) {
                    Map<String, Object> values = readMetaData(pkData, getColumnsForPK());

                    pkNames.add((String) values.get("PK_NAME"));
                }
            } finally {
                if (pkData != null) {
                    pkData.close();
                }
            }

            return pkNames.contains(index.getName());
        }
    }
}
