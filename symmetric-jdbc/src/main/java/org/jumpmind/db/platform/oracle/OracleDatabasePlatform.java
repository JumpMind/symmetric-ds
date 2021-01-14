package org.jumpmind.db.platform.oracle;

import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;
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
import org.jumpmind.db.model.Transaction;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.AbstractJdbcDatabasePlatform;
import org.jumpmind.db.platform.DatabaseNamesConstants;
import org.jumpmind.db.platform.PermissionResult;
import org.jumpmind.db.platform.PermissionResult.Status;
import org.jumpmind.db.platform.PermissionType;
import org.jumpmind.db.sql.IConnectionCallback;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.sql.JdbcSqlTemplate;
import org.jumpmind.db.sql.Row;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.sql.SqlTemplateSettings;

/*
 * Provides support for the Oracle platform.
 */
public class OracleDatabasePlatform extends AbstractJdbcDatabasePlatform {

    /* The standard Oracle jdbc driver. */
    public static final String JDBC_DRIVER = "oracle.jdbc.driver.OracleDriver";

    /* The old Oracle jdbc driver. */
    public static final String JDBC_DRIVER_OLD = "oracle.jdbc.dnlddriver.OracleDriver";

    /* The thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_THIN = "oracle:thin";

    /* The thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_OCI8 = "oracle:oci8";

    /* The old thin subprotocol used by the standard Oracle driver. */
    public static final String JDBC_SUBPROTOCOL_THIN_OLD = "oracle:dnldthin";

    /*
     * Creates a new platform instance.
     */
    public OracleDatabasePlatform(DataSource dataSource, SqlTemplateSettings settings) {
        super(dataSource, settings);
    }

    @Override
    protected OracleDdlBuilder createDdlBuilder() {
        return new OracleDdlBuilder();
    }

    @Override
    protected OracleDdlReader createDdlReader() {
        return new OracleDdlReader(this);
    }

    @Override
    protected OracleJdbcSqlTemplate createSqlTemplate() {
        return new OracleJdbcSqlTemplate(dataSource, settings, new OracleLobHandler(), getDatabaseInfo());
    }

    @Override
    protected ISqlTemplate createSqlTemplateDirty() {
        return sqlTemplate;
    }

    public String getName() {
        return DatabaseNamesConstants.ORACLE;
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        if (StringUtils.isBlank(defaultSchema)) {
            defaultSchema = (String) getSqlTemplate()
                    .queryForObject("SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }

    @Override
    public boolean canColumnBeUsedInWhereClause(Column column) {
        return !isLob(column.getJdbcTypeCode());
    }

    @Override
    public PermissionResult getCreateSymTriggerPermission() {
        String delimiter = getDatabaseInfo().getDelimiterToken();
        delimiter = delimiter != null ? delimiter : "";

        String triggerSql = "CREATE OR REPLACE TRIGGER TEST_TRIGGER AFTER UPDATE ON " + delimiter
                + PERMISSION_TEST_TABLE_NAME + delimiter + " BEGIN END";

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
    public PermissionResult getExecuteSymPermission() {
        String executeSql = "SELECT DBMS_LOB.GETLENGTH('TEST'), UTL_RAW.CAST_TO_RAW('TEST') FROM DUAL";

        PermissionResult result = new PermissionResult(PermissionType.EXECUTE, executeSql);

        try {
            getSqlTemplate().update(executeSql);
            result.setStatus(Status.PASS);
        } catch (SqlException e) {
            result.setException(e);
            result.setSolution("Grant EXECUTE on DBMS_LOB and UTL_RAW");
        }

        return result;
    }

    @Override
    protected PermissionResult getLogMinePermission() {
        String sql = "alter session set nls_date_format='YYYY-MM-DD HH24:MI:SS'";
        final PermissionResult result = new PermissionResult(PermissionType.LOG_MINE, "Use LogMiner");

        try {
            getSqlTemplate().update(sql);
        } catch (Exception e) {
            log.error("Error checking alter session permission", e);
            result.setStatus(Status.FAIL);
            result.setException(e);
            result.setSolution("Grant ALTER SESSION");
            return result;
        }

        return ((JdbcSqlTemplate) getSqlTemplate()).execute(new IConnectionCallback<PermissionResult>() {
            @Override
            public PermissionResult execute(Connection con) throws SQLException {
                Statement st = con.createStatement();
                boolean isBegin = false;
                try {
                    st.executeUpdate("BEGIN dbms_logmnr.start_logmnr(STARTSCN => timestamp_to_scn(sysdate), "
                            + "ENDSCN => timestamp_to_scn(sysdate), OPTIONS => DBMS_LOGMNR.CONTINUOUS_MINE);END;");
                    isBegin = true;
                } catch (Exception e) {
                    log.error("Error checking execute_catalog_role permission", e);
                    result.setStatus(Status.FAIL);
                    result.setException(e);
                    result.setSolution("Grant EXECUTE_CATALOG_ROLE");
                }

                if (isBegin) {
                    try {
                        st.execute("select * from V$LOGMNR_CONTENTS");
                        result.setStatus(Status.PASS);
                    } catch (Exception e) {
                        log.error("Error checking select_any_transaction permission", e);
                        result.setStatus(Status.FAIL);
                        result.setException(e);
                        result.setSolution("Grant SELECT ANY TRANSACTION, SELECT ANY DICTIONARY");
                    }
                    try {
                        st.executeUpdate("BEGIN dbms_logmnr.end_logmnr();END;");
                    } catch (Exception e) {
                    }
                }
                return result;
            }
        });
    }

    @Override
    public long getEstimatedRowCount(Table table) {
        return getSqlTemplateDirty().queryForLong(
                "select nvl(num_rows, -1) from all_tables where table_name = ? and owner = ?", table.getName(),
                table.getSchema());
    }
    
    @Override
    public List<Transaction> getTransactions() {
        ISqlTemplate template = getSqlTemplate();
        List<Transaction> transactions = new ArrayList<Transaction>();
        if (template.getDatabaseMajorVersion() >= 10) {
            String sql = "select" + 
                    "  blocked.sid," + 
                    "  blocked.username," + 
                    "  blocked.status," + 
                    "  blocked.blocking_session" + 
                    "  sql.last_load_time," + 
                    "  sql.sql_text " + 
                    "from v$session blocked " + 
                    "left join v$sqlarea sql" + 
                    "  on blocked.sql_id = sql.sql_id";
            for (Row row : template.query(sql)) {
                Transaction transaction = new Transaction(row.getString("sid"), row.getString("username"),
                        row.getString("blocking_session"), row.getDateTime("last_load_time"), row.getString("sql_text"));
                transaction.setStatus(row.getString("status"));
                transactions.add(transaction);
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
        
        int orderIndex = StringUtils.lastIndexOfIgnoreCase(sql, "order by");
        String order = sql.substring(orderIndex);
        String innerSql = sql.substring(0, orderIndex - 1);
        innerSql = StringUtils.replaceIgnoreCase(innerSql, " from", ", ROW_NUMBER() over (" + order + ") as row_num from");
        return "select * from (" + innerSql + ") " +
               "where row_num between " + (offset + 1) + " and " + (offset + limit);
    }

}
