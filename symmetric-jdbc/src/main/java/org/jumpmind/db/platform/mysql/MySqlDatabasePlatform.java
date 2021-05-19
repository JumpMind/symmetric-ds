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
package org.jumpmind.db.platform.mysql;

import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

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

import javax.sql.DataSource;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Database;
import org.jumpmind.db.model.Transaction;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * The platform implementation for MySQL.
 */
public class MySqlDatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard MySQL jdbc driver. */
    public static final String JDBC_DRIVER = "com.mysql.jdbc.Driver";

    /* The old MySQL jdbc driver. */
    public static final String JDBC_DRIVER_OLD = "org.gjt.mm.mysql.Driver";

    /* The subprotocol used by the standard MySQL driver. */
    public static final String JDBC_SUBPROTOCOL = "mysql";

    /*
     * Creates a new platform instance.
     */
    public MySqlDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, overrideSettings(settings));
    }

    @Override
    protected MySqlDdlBuilder createDdlBuilder() {
        return new MySqlDdlBuilder();
    }

    @Override
    protected MySqlDdlReader createDdlReader() {
        return new MySqlDdlReader(this);
    }

    @Override
    protected MySqlJdbcSqlTemplate createSqlTemplate() {
        return new MySqlJdbcSqlTemplate(dataSource, settings, null, getDatabaseInfo());
    }

    /*
     * According to the documentation (and experience) the jdbc driver for mysql
     * requires the fetch size to be as follows.
     */
    protected static SqlTemplateSettings overrideSettings(SqlTemplateSettings settings) {
        if (settings == null) {
            settings = new SqlTemplateSettings();
        }
        settings.setFetchSize(Integer.MIN_VALUE);
        return settings;
    }

    public String getName() {
        return DatabaseNamesConstants.MYSQL;
    }

    public String getDefaultSchema() {
        return null;
    }

    public String getDefaultCatalog() {
        if (StringUtils.isBlank(defaultCatalog)) {
            defaultCatalog = getSqlTemplate().queryForObject("select database()", String.class);
        }
        return defaultCatalog;
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";

        String triggerSql = "CREATE TRIGGER TEST_TRIGGER AFTER UPDATE ON " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter
                + " FOR EACH ROW INSERT INTO " + delimiter + PERMISSION_TEST_TABLE_NAME + delimiter + " VALUES(NULL,NULL)";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_TRIGGER, triggerSql);

        try {
            getSqlTemplate().update(triggerSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE TRIGGER permission or TRIGGER permission");
        }

        return result;
    }

    @Override
    public PermissionResult getCreateSymRoutinePermission() {
        String routineSql = "CREATE PROCEDURE TEST_PROC() BEGIN SELECT 1; END";
        String dropSql = "DROP PROCEDURE IF EXISTS TEST_PROC";

        PermissionResult result = new PermissionResult(PermissionType.CREATE_ROUTINE, 
                dropSql + "\r\n" + routineSql + "\r\n" + dropSql);

        try {
            getSqlTemplate().update(dropSql);
            getSqlTemplate().update(routineSql);
            getSqlTemplate().update(dropSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant CREATE ROUTINE Privilege");
        }
        return result;
    }
    
    @Override
    public void makePlatformSpecific(Database database) {
        for (Table table : database.getTables()) {
            for (Column column : table.getColumns()) {
                try {
                    if (column.getMappedTypeCode() == Types.DATE 
                            && column.findPlatformColumn(DatabaseNamesConstants.ORACLE) != null
                            && column.findPlatformColumn(DatabaseNamesConstants.ORACLE122) != null) {
                        column.setMappedType(TypeMap.TIMESTAMP);
                        column.setMappedTypeCode(Types.TIMESTAMP);
                        column.setScale(6);
                    }
                }
                catch (Exception e) {}
            }
        }
        super.makePlatformSpecific(database);
    }
    
    @Override
    public long getEstimatedRowCount(Table table) {        
        return getSqlTemplateDirty().queryForLong("select ifnull(table_rows,-1) from information_schema.tables where table_name = ? and table_schema = ?",
                table.getName(), table.getCatalog());
    }
    
    @Override
    public boolean canColumnBeUsedInWhereClause(Column column) {
        if ((column.getMappedTypeCode() == Types.VARBINARY && column.getSizeAsInt() <= 8000)
                || column.getMappedTypeCode() == Types.BINARY) {
            return true;
        }
        return !column.isOfBinaryType();
    }
    
    @Override
    public List<Transaction> getTransactions() {
        ISqlTemplate template = getSqlTemplate();
        String transactionString = "trx";
        String lockWaitsString = "information_schema.innodb_lock_waits";
        if (template.getDatabaseMajorVersion() >= 8) {
            transactionString = "engine_transaction";
            lockWaitsString = "performance_schema.data_lock_waits";
        }
        List<Transaction> transactions = new ArrayList<Transaction>();
        if (template.getDatabaseMajorVersion() == 5 && template.getDatabaseMinorVersion() <= 5) {
            String sql = "SELECT" + 
                    "  b.trx_id," + 
                    "  b.trx_started," + 
                    "  b.trx_state," + 
                    "  b.trx_rows_modified," + 
                    "  w.blocking_" + transactionString + "_id 'blockingId'," + 
                    "  b.trx_query " + 
                    "FROM " + lockWaitsString + " w " + 
                    "RIGHT JOIN information_schema.innodb_trx b" + 
                    "  ON b.trx_id = w.requesting_" + transactionString + "_id;";
            for (Row row : template.query(sql)) {
                Transaction transaction = new Transaction(row.getString("trx_id"), "",
                        row.getString("blockingId"), row.getDateTime("trx_started"), row.getString("trx_query"));
                transaction.setStatus(row.getString("trx_state"));
                transaction.setWrites(row.getInt("trx_rows_modified"));
                transactions.add(transaction);
            }
            return transactions;
        }
        String sql = "SELECT" + 
                "  b.trx_id," + 
                "  t.processlist_user," + 
                "  t.processlist_host," + 
                "  b.trx_started," + 
                "  b.trx_state," + 
                "  b.trx_rows_modified," + 
                "  w.blocking_" + transactionString + "_id 'blockingId'," + 
                "  b.trx_query " + 
                "FROM " + lockWaitsString + " w " + 
                "RIGHT JOIN information_schema.innodb_trx b" + 
                "  ON b.trx_id = w.requesting_" + transactionString + "_id " + 
                "INNER JOIN performance_schema.threads t" + 
                "  ON b.trx_mysql_thread_id = t.thread_id;";
        for (Row row : template.query(sql)) {
            Transaction transaction = new Transaction(row.getString("trx_id"), row.getString("processlist_user"),
                    row.getString("blockingId"), row.getDateTime("trx_started"), row.getString("trx_query"));
            transaction.setRemoteHost(row.getString("processlist_host"));
            transaction.setStatus(row.getString("trx_state"));
            transaction.setWrites(row.getInt("trx_rows_modified"));
            transactions.add(transaction);
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
        return sql + " limit " + offset + "," + limit;
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
