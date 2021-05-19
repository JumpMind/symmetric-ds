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

import java.sql.Array;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.sql.DataSource;
import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Transaction;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;
import org.jumpmind.db.sql.SymmetricLobHandler;
import org.jumpmind.db.util.BinaryEncoding;

/*
 * The platform implementation for PostgresSql.
 */
public class PostgreSqlDatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard PostgreSQL jdbc driver. */
    public static final String JDBC_DRIVER = "org.postgresql.Driver";
    
    /* The subprotocol used by the standard PostgreSQL driver. */
    public static final String JDBC_SUBPROTOCOL = "postgresql";       

    /*
     * Creates a new platform instance.
     */
    public PostgreSqlDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, overrideSettings(settings));
    }   
    
    protected static SqlTemplateSettings overrideSettings(SqlTemplateSettings settings) {
        if (settings == null) {
            settings = new SqlTemplateSettings();
        }
        // Query timeout needs to be zero for postrgres because the jdbc driver does
        // not support a timeout setting of of other than zero.
        settings.setQueryTimeout(0);
        return settings;
    }

    protected static boolean isBlobStoredByReference(String jdbcTypeName) {
        if ("OID".equalsIgnoreCase(jdbcTypeName) || "LO".equalsIgnoreCase(jdbcTypeName)) {
            return true;
        } else {
            return false;
        }
    }
    
    @Override
    protected PostgreSqlDdlBuilder createDdlBuilder() {
        return new PostgreSqlDdlBuilder();
    }

    @Override
    protected PostgreSqlDdlReader createDdlReader() {
        return new PostgreSqlDdlReader(this);
    }    
    
    @Override
    protected PostgreSqlJdbcSqlTemplate createSqlTemplate() {
        SymmetricLobHandler lobHandler = new PostgresLobHandler();
        return new PostgreSqlJdbcSqlTemplate(dataSource, settings, lobHandler, getDatabaseInfo());
    }

    public String getName() {
        return DatabaseNamesConstants.POSTGRESQL;
    }
    
    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate().queryForObject("select current_schema()", String.class);
        }
        return defaultSchema;
    }
    
    public String getDefaultCatalog() {
        return null;
    }    

    @Override
    protected Array createArray(Column column, final String value) {
        if (StringUtils.isNotBlank(value)) {

            String jdbcTypeName = column.getJdbcTypeName();
            if (jdbcTypeName.startsWith("_")) {
                jdbcTypeName = jdbcTypeName.substring(1);
            }
            int jdbcBaseType = Types.VARCHAR;
            if (jdbcTypeName.toLowerCase().contains("int")) {
                jdbcBaseType = Types.INTEGER;
            }
                        
            final String baseTypeName = jdbcTypeName;
            final int baseType = jdbcBaseType;
            return new Array() {
                public String getBaseTypeName() {
                    return baseTypeName;
                }

                public void free() {
                }

                public int getBaseType() {
                    return baseType;
                }

                public Object getArray() {
                    return null;
                }

                public Object getArray(Map<String, Class<?>> map) {
                    return null;
                }

                public Object getArray(long index, int count) {
                    return null;
                }

                public Object getArray(long index, int count, Map<String, Class<?>> map) {
                    return null;
                }

                public ResultSet getResultSet() {
                    return null;
                }

                public ResultSet getResultSet(Map<String, Class<?>> map) {
                    return null;
                }

                public ResultSet getResultSet(long index, int count) {
                    return null;
                }

                public ResultSet getResultSet(long index, int count, Map<String, Class<?>> map) {
                    return null;
                }

                public String toString() {
                    return value;
                }
            };
        } else {
            return null;
        }
    }
    
    @Override
    protected String cleanTextForTextBasedColumns(String text) {
        return text.replace("\0", "");
    }    
    
    @Override
    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
            Column[] orderedMetaData, boolean useVariableDates, boolean fitToColumn) {
        Object[] objectValues = super.getObjectValues(encoding, values, orderedMetaData, useVariableDates, fitToColumn);
        for (int i = 0; i < orderedMetaData.length; i++) {
            if (orderedMetaData[i] != null && orderedMetaData[i].getMappedTypeCode() == Types.BLOB
                    && objectValues[i] != null) {
                try {
                    objectValues[i] = new SerialBlob((byte[]) objectValues[i]);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }                
            }
        }
        return objectValues;
    }
    
    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";

        String triggerSql = "CREATE OR REPLACE FUNCTION TEST_TRIGGER() RETURNS trigger AS $$ BEGIN END $$ LANGUAGE plpgsql";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, triggerSql);

        try {
            getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission and/or DROP TRIGGER permission");
        }

        return result;
    }
    
    @Override
    public PermissionResult getDropSymTriggerPermission() {
        String dropTriggerSql = "DROP FUNCTION TEST_TRIGGER()";

        PermissionResult result = new PermissionResult(PermissionType.DROP_TRIGGER, dropTriggerSql);

        try {
            getSqlTemplate().update(dropTriggerSql);
            result.setStatus(PermissionResult.Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
        }

        return result;
    }

    @Override
    public long getEstimatedRowCount(Table table) {        
        return getSqlTemplateDirty().queryForLong("select coalesce(c.reltuples, -1) from pg_catalog.pg_class c inner join pg_catalog.pg_namespace n " +
                "on n.oid = c.relnamespace where c.relname = ? and n.nspname = ?",
                table.getName(), table.getSchema());
    }
    
    @Override
    public String getTruncateSql(Table table) {
        String sql = super.getTruncateSql(table);
        sql += " cascade";
        return sql;
    }

    @Override
    public String getDeleteSql(Table table) {
        String sql = super.getDeleteSql(table);
        sql += " cascade";
        return sql;
    }
    
    @Override
    public List<Transaction> getTransactions() {
        ISqlTemplate template = getSqlTemplate();
        String sql = "";
        boolean oldVersion = (template.getDatabaseMajorVersion() == 9 && template.getDatabaseMinorVersion() < 2)
                || template.getDatabaseMajorVersion() < 9;
        if (oldVersion) {
            sql = "select distinct" + 
                    "  a.procpid as pid," + 
                    "  a.usename," + 
                    "  a.client_addr," + 
                    "  blocking.pid as blockingPid," + 
                    "  a.query_start," + 
                    "  a.current_query as query " + 
                    "from pg_stat_activity a " + 
                    "join pg_catalog.pg_locks blocked" + 
                    "  on a.procpid = blocked.pid " + 
                    "left join pg_catalog.pg_locks blocking" + 
                    "  on (blocking.relation = blocked.relation and blocking.locktype = blocked.locktype and blocked.pid != blocking.pid)";
        } else {
            sql = "select distinct" + 
                    "  a.pid as pid," + 
                    "  a.usename," + 
                    "  a.client_addr," + 
                    "  a.client_hostname," + 
                    "  a.state," + 
                    "  blocking.pid as blockingPid," + 
                    "  a.query_start," + 
                    "  a.query as query " + 
                    "from pg_stat_activity a " + 
                    "join pg_catalog.pg_locks blocked" + 
                    "  on a.pid = blocked.pid " + 
                    "left join pg_catalog.pg_locks blocking" + 
                    "  on (blocking.relation = blocked.relation and blocking.locktype = blocked.locktype and blocked.pid != blocking.pid)";
        }
        List<Transaction> transactions = new ArrayList<Transaction>();
        final String formatString = "yyyy-MM-dd HH:mm:ss.SSSX";
        final SimpleDateFormat dateFormat = new SimpleDateFormat(formatString);
        for (Row row : template.query(sql)) {
            try {
                String startTime = row.getString("query_start");
                String adjustedTime;
                if (startTime != null) {
                    int decimalIndex = startTime.indexOf(".");
                    int timezoneIndex = startTime.indexOf("+");
                    if (timezoneIndex == -1) {
                        timezoneIndex = startTime.indexOf("Z");
                        if (timezoneIndex == -1) {
                            timezoneIndex = startTime.lastIndexOf("-");
                        }
                    }
                    adjustedTime = StringUtils.rightPad(startTime.substring(0, Math.min(decimalIndex + 4, timezoneIndex)), 3, "0")
                            + startTime.substring(timezoneIndex);
                } else {
                    continue;
                }
                Transaction transaction = new Transaction(row.getString("pid"), row.getString("usename"), row.getString("blockingPid"),
                        dateFormat.parse(adjustedTime), row.getString("query"));
                transaction.setRemoteIp(row.getString("client_addr"));
                if (!oldVersion) {
                    transaction.setRemoteHost(row.getString("client_hostname"));
                    transaction.setStatus(row.getString("state"));
                }
                transactions.add(transaction);
            } catch (ParseException e) {
                log.error("Could not parse date", e);
            }

        }
        return transactions;
    }
    
    @Override
    public boolean supportsLimitOffset() {
        return true;
    }
    
    @Override
    public String massageForLimitOffset(String sql, int limit, int offset) {
        if (sql.endsWith(";")) {
            sql = sql.substring(0, sql.length() - 1);
        }
        return sql + " limit " + limit + " offset " + offset;
    }
    
    @Override
    public boolean supportsSliceTables() {
        return true;
    }
    
    @Override
    public String getSliceTableSql(String columnName, int sliceNum, int totalSlices) {
        return "ascii(substring(" + columnName + ", 1, 1)) % " + totalSlices + " = " + sliceNum;
    }

}
