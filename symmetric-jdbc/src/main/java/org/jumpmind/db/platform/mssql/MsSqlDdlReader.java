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

import static org.apache.commons.lang3.StringUtils.isNotBlank;

import java.sql.Connection;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.CompressionTypes;
import org.jumpmind.db.model.IIndex;
import org.jumpmind.db.model.PlatformColumn;
import org.jumpmind.db.model.PlatformIndex;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.Trigger;
import org.jumpmind.db.model.Trigger.TriggerType;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDdlReader;
import org.jumpmind.db.platform.DatabaseMetaDataWrapper;
import org.jumpmind.db.platform.DdlException;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ChangeCatalogConnectionHandler;
import org.jumpmind.db.sql.IConnectionHandler;
import org.jumpmind.db.sql.ISqlRowMapper;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.mapper.StringMapper;

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

    private Set<String> userDefinedDataTypes;
    
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
            if (StringUtils.equalsIgnoreCase(table.getSchema(), "sys")) {
                return null;
            }
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
            if (platform instanceof MsSql2008DatabasePlatform) {
                JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
                String sql = "SELECT [TABLENAME] = t.[Name]\n" +
                        "        ,[INDEXNAME] = i.[Name]\n" +
                        "        ,[IndexType] = i.[type_desc]\n" +
                        "        ,[FILTER] = i.filter_definition\n" +
                        "        ,[HASFILTER] = i.has_filter\n" +
                        "        ,[COMPRESSIONTYPE] = p.data_compression\n" +
                        "        ,[COMPRESSIONDESCRIPTION] = p.data_compression_desc\n" +
                        "FROM sys.indexes i\n" +
                        "INNER JOIN sys.tables t ON t.object_id = i.object_id\n" +
                        "INNER JOIN sys.partitions p ON p.object_id=t.object_id AND p.index_id=i.index_id\n" +
                        "WHERE t.type_desc = N'USER_TABLE'\n" +
                        "and t.name=?\n" +
                        "and p.index_id in (0,1)";
                List<String> l = new ArrayList<String>();
                l.add(tableName);
                log.debug("Running the following query to get metadata about whether a table has compression\n {}", sql);
                List<Row> filters = sqlTemplate.query(sql, l.toArray());
                for (Row filter : filters) {
                    int compressionType = filter.getInt("COMPRESSIONTYPE");
                    boolean hasCompression = (compressionType > 0);
                    if (hasCompression) {
                        if (compressionType == 1) {
                            log.debug("table: " + tableName + " has compression: " + CompressionTypes.ROW.name());
                            table.setCompressionType(CompressionTypes.ROW);
                        } else if (compressionType == 2) {
                            log.debug("table: " + tableName + " has compression: " + CompressionTypes.PAGE.name());
                            table.setCompressionType(CompressionTypes.PAGE);
                        } else if (compressionType == 3) {
                            log.debug("table: " + tableName + " has compression: " + CompressionTypes.COLUMNSTORE.name());
                            table.setCompressionType(CompressionTypes.COLUMNSTORE);
                        } else if (compressionType == 4) {
                            log.debug("table: " + tableName + " has compression: " + CompressionTypes.COLUMNSTORE_ARCHIVE.name());
                            table.setCompressionType(CompressionTypes.COLUMNSTORE_ARCHIVE);
                        } else {
                            table.setCompressionType(CompressionTypes.NONE);
                        }
                    }
                }
            }
        }
        return table;
    }

    @Override
    protected boolean isInternalPrimaryKeyIndex(Connection connection,
            DatabaseMetaDataWrapper metaData, Table table, IIndex index) {
        // Sql Server generates an index "PK__[table name]__[hex number]"
        StringBuilder pkIndexName = new StringBuilder();
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

    @Override
    protected Integer mapUnknownJdbcTypeForColumn(Map<String, Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        int size = -1;
        String columnSize = (String) values.get("COLUMN_SIZE");
        if (isNotBlank(columnSize)) {
            size = Integer.parseInt(columnSize);
        }
        if (typeName != null) {
            if (typeName.toLowerCase().startsWith("text")) {
                return Types.LONGVARCHAR;
            } else if (typeName.toLowerCase().startsWith("ntext")) {
                return Types.NCLOB;
            } else if (typeName.toLowerCase().equals("float")) {
                return Types.FLOAT;
            } else if (typeName.toUpperCase().contains(TypeMap.GEOMETRY)) {
                return Types.VARCHAR;
            } else if (typeName.toUpperCase().contains(TypeMap.GEOGRAPHY)) {
                return Types.VARCHAR;
            } else if (typeName.toUpperCase().contains("VARCHAR") && size > 8000) {
                return Types.LONGVARCHAR;
            } else if (typeName.toUpperCase().contains("NVARCHAR") && size > 4000) {
                return Types.LONGNVARCHAR;
            } else if (typeName.toUpperCase().equals("SQL_VARIANT")) {
                return Types.BINARY;
            } else if (typeName.equalsIgnoreCase("DATETIMEOFFSET")) {
                return ColumnTypes.TIMESTAMPTZ;
            } else if (typeName.equalsIgnoreCase("datetime2")) {
                return Types.TIMESTAMP;
            } else if (typeName.equalsIgnoreCase("DATE")) {
                return Types.DATE;
            } else if (typeName.equalsIgnoreCase("TIME")) {
                return Types.TIME;
            } else if (typeName.equalsIgnoreCase("VARBINARY") && size > 8000) {
                return Types.BLOB;
            }
        }
        return super.mapUnknownJdbcTypeForColumn(values);
    }

    @Override
    protected Column readColumn(DatabaseMetaDataWrapper metaData, Map<String, Object> values)
            throws SQLException {
        Column column = super.readColumn(metaData, values);
        String defaultValue = column.getDefaultValue();
        
        if (userDefinedDataTypes == null) {
        	userDefinedDataTypes = new HashSet<String>();
        	JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
        	if (sqlTemplate.getDatabaseMajorVersion() >= 9) {
            	String sql = "select name from sys.types where is_user_defined = 1";
	            List<Row> rows = sqlTemplate.query(sql);
	            for (Row r : rows) {
	            	userDefinedDataTypes.add(r.getString("name"));
	            }
        	}
        }
        
        if (column.isGenerated() && defaultValue == null) {
            JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
            String sql = "SELECT definition\n"
                    + "FROM sys.computed_columns\n"
                    + "WHERE OBJECT_NAME(object_id) = ?\n"
                    + "AND name = ?";
            List<String> l = new ArrayList<String>();
            l.add((String) values.get("TABLE_NAME"));
            l.add(column.getName());
            String definition = sqlTemplate.queryForString(sql, l.toArray());
            column.setDefaultValue(definition);
        }
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
                    defaultValue = defaultValue.substring(2, defaultValue.length() - 1);
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
                        || column.getJdbcTypeName().equals("uniqueidentifier")
                        || column.getJdbcTypeName().equals("date"))
                || column.getJdbcTypeName().equals("timestamp")) {
            removePlatformColumnSize(column);
        }
        if (column.getJdbcTypeName() != null) {
            if (column.getJdbcTypeName().equalsIgnoreCase("datetime2")) {
                adjustColumnSize(column, -20);
            } else if (column.getJdbcTypeName().equalsIgnoreCase("datetime")) {
                column.setSize("3");
                removePlatformColumnSize(column);
            } else if (column.getJdbcTypeName().equalsIgnoreCase("time")) {
                adjustColumnSize(column, -9);
            } else if (column.getJdbcTypeName().equalsIgnoreCase("datetimeoffset")) {
                adjustColumnSize(column, -27);
            } else if (column.getJdbcTypeName().equalsIgnoreCase("date") || column.getJdbcTypeName().equalsIgnoreCase("smalldatetime")) {
                removeColumnSize(column);
            }
        }
        
        if (userDefinedDataTypes.size() > 0) {
        	if(userDefinedDataTypes.contains(column.getJdbcTypeName())) {
        		removePlatformColumnSize(column);
        		for (PlatformColumn pc : column.getPlatformColumns().values()) {
        			pc.setUserDefinedType(true);
        		}
        	}
        }
        return column;
    }

    @Override
    public List<String> getTableNames(final String catalog, final String schema,
            final String[] tableTypes) {
        StringBuilder sql = new StringBuilder("select \"TABLE_NAME\" from ");
        if (isNotBlank(catalog)) {
            sql.append("\"").append(catalog).append("\"").append(".");
        }
        sql.append("\"INFORMATION_SCHEMA\".\"TABLES\" where \"TABLE_TYPE\"='BASE TABLE'");
        List<Object> args = new ArrayList<Object>(2);
        if (isNotBlank(catalog)) {
            sql.append(" and \"TABLE_CATALOG\"=?");
            args.add(catalog);
        }
        if (isNotBlank(schema)) {
            sql.append(" and \"TABLE_SCHEMA\"=?");
            args.add(schema);
        }
        return schema == null ? new ArrayList<String>()
                : platform.getSqlTemplateDirty().queryWithHandler(sql.toString(), new StringMapper(),
                        new ChangeCatalogConnectionHandler(catalog), args.toArray(new Object[args.size()]));
    }

    @Override
    public List<Trigger> getTriggers(final String catalog, final String schema,
            final String tableName) throws SqlException {
        log.debug("Reading triggers for: " + tableName);
        JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
        String sql = "select "
                + "TRIG.name, "
                + "TAB.name as table_name, "
                + "SC.name as table_schema, "
                + "TRIG.is_disabled, "
                + "TRIG.is_ms_shipped, "
                + "TRIG.is_not_for_replication, "
                + "TRIG.is_instead_of_trigger, "
                + "TRIG.create_date, "
                + "TRIG.modify_date, "
                + "OBJECTPROPERTY(TRIG.OBJECT_ID, 'ExecIsUpdateTrigger') AS isupdate, "
                + "OBJECTPROPERTY(TRIG.OBJECT_ID, 'ExecIsDeleteTrigger') AS isdelete, "
                + "OBJECTPROPERTY(TRIG.OBJECT_ID, 'ExecIsInsertTrigger') AS isinsert, "
                + "OBJECTPROPERTY(TRIG.OBJECT_ID, 'ExecIsAfterTrigger') AS isafter, "
                + "OBJECTPROPERTY(TRIG.OBJECT_ID, 'ExecIsInsteadOfTrigger') AS isinsteadof, "
                + "TRIG.object_id, "
                + "TRIG.parent_id, "
                + "TAB.schema_id, "
                + "OBJECT_DEFINITION(TRIG.OBJECT_ID) as trigger_source "
                + "from sys.triggers as TRIG "
                + "inner join sys.tables as TAB "
                + "on TRIG.parent_id = TAB.object_id "
                + "inner join sys.schemas as SC "
                + "on TAB.schema_id = SC.schema_id "
                + "where TAB.name=? and SC.name=? ";
        return sqlTemplate.queryWithHandler(sql, new ISqlRowMapper<Trigger>() {
            public Trigger mapRow(Row row) {
                Trigger trigger = new Trigger();
                trigger.setName(row.getString("name"));
                trigger.setSchemaName(row.getString("table_schema"));
                trigger.setTableName(row.getString("table_name"));
                trigger.setEnabled(!Boolean.valueOf(row.getString("is_disabled")));
                trigger.setSource(row.getString("trigger_source"));
                row.remove("trigger_source");
                // replace 0 and 1s with true and false
                for (String s : new String[] { "isupdate", "isdelete", "isinsert", "isafter", "isinsteadof" }) {
                    if (row.getString(s).equals("0"))
                        row.put(s, false);
                    else
                        row.put(s, true);
                }
                if (row.getBoolean("isupdate"))
                    trigger.setTriggerType(TriggerType.UPDATE);
                else if (row.getBoolean("isdelete"))
                    trigger.setTriggerType(TriggerType.DELETE);
                else if (row.getBoolean("isinsert"))
                    trigger.setTriggerType(TriggerType.INSERT);
                trigger.setMetaData(row);
                return trigger;
            }
        }, new ChangeCatalogConnectionHandler(catalog), tableName, schema);
    }

    protected IConnectionHandler getConnectionHandler(String catalog) {
        return new ChangeCatalogConnectionHandler(catalog == null ? platform.getDefaultCatalog() : catalog);
    }

    @Override
    protected Collection<IIndex> readIndices(Connection connection,
            DatabaseMetaDataWrapper metaData, String tableName) throws SQLException {
        Collection<IIndex> cIndex = super.readIndices(connection, metaData, tableName);
        if (platform instanceof MsSql2008DatabasePlatform) {
            if (cIndex != null && cIndex.size() > 0) {
                JdbcSqlTemplate sqlTemplate = (JdbcSqlTemplate) platform.getSqlTemplateDirty();
                String sql = "SELECT [TABLENAME] = t.[Name]\n" +
                        "        ,[INDEXNAME] = i.[Name]\n" +
                        "        ,[IndexType] = i.[type_desc]\n" +
                        "        ,[FILTER] = i.filter_definition\n" +
                        "        ,[HASFILTER] = i.has_filter\n" +
                        "        ,[COMPRESSIONTYPE] = p.data_compression\n" +
                        "        ,[COMPRESSIONDESCRIPTION] = p.data_compression_desc\n" +
                        "FROM sys.indexes i\n" +
                        "INNER JOIN sys.tables t ON t.object_id = i.object_id\n" +
                        "INNER JOIN sys.partitions p ON p.object_id=t.object_id AND p.index_id=i.index_id\n" +
                        "WHERE t.type_desc = N'USER_TABLE'\n" +
                        "and t.name=?\n" +
                        "and i.name in (%s)\n" +
                        "and p.index_id > 1";
                StringBuilder sb = new StringBuilder();
                for (int i = 0; i < cIndex.size(); i++) {
                    if (sb.length() > 0) {
                        sb.append(",");
                    }
                    sb.append("?");
                }
                sql = String.format(sql, sb.toString());
                List<String> l = new ArrayList<String>();
                l.add(tableName);
                for (IIndex index : cIndex) {
                    l.add(index.getName());
                }
                log.debug("Running the following query to get metadata about whether an index has compression or filters\n {}", sql);
                List<Row> filters = sqlTemplate.query(sql, l.toArray());
                for (Row filter : filters) {
                    String indexName = filter.getString("INDEXNAME");
                    IIndex iIndex = findIndex(indexName, cIndex);
                    if (iIndex != null) {
                        boolean hasFilter = filter.getBoolean("HASFILTER");
                        int compressionType = filter.getInt("COMPRESSIONTYPE");
                        boolean hasCompression = (compressionType > 0);
                        if (hasFilter || hasCompression) {
                            PlatformIndex platformIndex = new PlatformIndex();
                            platformIndex.setName(indexName);
                            if (hasFilter) {
                                log.debug("table: " + tableName + " index: " + indexName + " has filter: " + filter.getString("FILTER"));
                                platformIndex.setFilterCondition("WHERE " + filter.getString("FILTER"));
                            }
                            if (hasCompression) {
                                if (compressionType == 1) {
                                    log.debug("table: " + tableName + " index: " + indexName + " has compression: " + CompressionTypes.ROW.name());
                                    platformIndex.setCompressionType(CompressionTypes.ROW);
                                } else if (compressionType == 2) {
                                    log.debug("table: " + tableName + " index: " + indexName + " has compression: " + CompressionTypes.PAGE.name());
                                    platformIndex.setCompressionType(CompressionTypes.PAGE);
                                } else if (compressionType == 3) {
                                    log.debug("table: " + tableName + " index: " + indexName + " has compression: " + CompressionTypes.COLUMNSTORE.name());
                                    platformIndex.setCompressionType(CompressionTypes.COLUMNSTORE);
                                } else if (compressionType == 4) {
                                    log.debug("table: " + tableName + " index: " + indexName + " has compression: " + CompressionTypes.COLUMNSTORE_ARCHIVE
                                            .name());
                                    platformIndex.setCompressionType(CompressionTypes.COLUMNSTORE_ARCHIVE);
                                } else {
                                    platformIndex.setCompressionType(CompressionTypes.NONE);
                                }
                            }
                            iIndex.addPlatformIndex(platformIndex);
                        }
                    }
                }
            }
        }
        return cIndex;
    }

    private IIndex findIndex(String indexName, Collection<IIndex> cIndex) {
        for (IIndex index : cIndex) {
            if (StringUtils.equals(index.getName(), indexName)) {
                return index;
            }
        }
        return null;
    }

    protected String getWithNoLockHint() {
        return " WITH (NOLOCK) ";
    }
}
