/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
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

import static org.jumpmind.db.model.ColumnTypes.MAPPED_TIMESTAMPTZ;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ForeignKey;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;

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
            HashMap<String, IIndex> uniquesByName = new HashMap<String, IIndex>();
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
            setPrimaryKeyConstraintName(connection, table);
        }
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
        Integer type = (Integer) values.get("DATA_TYPE");
        if (typeName != null && typeName.equalsIgnoreCase("ABSTIME")) {
            return Types.TIMESTAMP;
        } else if (typeName != null && typeName.equalsIgnoreCase("TIMESTAMPTZ")) {
            // lets use the same type code that oracle uses
            return MAPPED_TIMESTAMPTZ;
        } else if (PostgreSqlDatabasePlatform.isBlobStoredByReference(typeName)) {
            return Types.BLOB;
        } else if (type != null && (type == Types.STRUCT || type == Types.OTHER)) {
            return Types.LONGVARCHAR;
        } else if (typeName != null && typeName.equalsIgnoreCase("BIT")) {
            return Types.VARCHAR;
        } else {
            return super.mapUnknownJdbcTypeForColumn(values);
        }
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values) throws SQLException {
        Column column = super.readColumn(metaData, values);
        PlatformColumn platformColumn = column.findPlatformColumn(platform.getName());
        if (platformColumn != null) {
            if ("serial".equals(platformColumn.getType()) || "serial4".equals(platformColumn.getType())) {
                platformColumn.setType("int4");
            } else if ("bigserial".equals(platformColumn.getType()) || "serial8".equals(platformColumn.getType())) {
                platformColumn.setType("int8");
            }
        }
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
                if (column.getMappedTypeCode() == Types.VARCHAR) {
                    if (column.getJdbcTypeName().equalsIgnoreCase("TEXT")) {
                        column.setMappedTypeCode(Types.LONGVARCHAR);
                        column.setSize(null);
                    }
                } else if (column.getMappedTypeCode() == Types.BINARY) {
                    column.setMappedTypeCode(Types.LONGVARBINARY);
                    column.setSize(null);
                }
            } else if (column.getSizeAsInt() == 131089 && column.getJdbcTypeCode() == Types.NUMERIC) {
                column.setSizeAndScale(0, 0);
                column.setMappedTypeCode(Types.DECIMAL);
                if (platformColumn != null) {
                    platformColumn.setSize(-1);
                    platformColumn.setDecimalDigits(-1);
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
     * Extractes the default value from a default value spec of the form "'some value'::character varying" or "'2000-01-01'::date".
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
     * Extractes the default value from a default value spec of the form "-9000000000000000000::bigint".
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

    @Override
    public List<Trigger> getTriggers(final String catalog, final String schema,
            final String tableName) {
        List<Trigger> triggers = new ArrayList<Trigger>();
        log.debug("Reading triggers for: " + tableName);
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform
                .getSqlTemplate();
        String sql = "SELECT "
                + "trigger_name, "
                + "trigger_schema, "
                + "trigger_catalog, "
                + "event_manipulation AS trigger_type, "
                + "event_object_table AS table_name,"
                + "trig.*, "
                + "pgproc.prosrc "
                + "FROM INFORMATION_SCHEMA.TRIGGERS AS trig "
                + "INNER JOIN pg_catalog.pg_trigger AS pgtrig "
                + "ON pgtrig.tgname=trig.trigger_name "
                + "INNER JOIN pg_catalog.pg_proc AS pgproc "
                + "ON pgproc.oid=pgtrig.tgfoid "
                + "WHERE event_object_table=? AND event_object_schema=?;";
        triggers = sqlTemplate.query(sql, new ISqlRowMapper<Trigger>() {
            public Trigger mapRow(Row row) {
                Trigger trigger = new Trigger();
                trigger.setName(row.getString("trigger_name"));
                trigger.setCatalogName(row.getString("trigger_catalog"));
                trigger.setSchemaName(row.getString("trigger_schema"));
                trigger.setTableName(row.getString("table_name"));
                trigger.setEnabled(true);
                trigger.setSource(row.getString("prosrc"));
                row.remove("prosrc");
                String triggerType = row.getString("trigger_type");
                if (triggerType.equals("DELETE")
                        || triggerType.equals("INSERT")
                        || triggerType.equals("UPDATE")) {
                    trigger.setTriggerType(TriggerType.valueOf(triggerType));
                }
                trigger.setMetaData(row);
                return trigger;
            }
        }, tableName, schema);
        return triggers;
    }
}
