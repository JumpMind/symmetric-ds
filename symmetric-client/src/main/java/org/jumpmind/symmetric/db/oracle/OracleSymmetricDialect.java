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
package org.jumpmind.symmetric.db.oracle;

import java.text.ParseException;
import java.util.Date;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlException;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * A dialect that is specific to Oracle databases
 */
public class OracleSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    static final String SQL_SELECT_TRIGGERS = "from ALL_TRIGGERS where owner in (SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual) and trigger_name like upper(?) and table_name like upper(?)";

    static final String SQL_SELECT_TRANSACTIONS = "select min(start_time) from gv$transaction where status = 'ACTIVE'";

    static final String SQL_OBJECT_INSTALLED = "select count(*) from user_source where line = 1 and (((type = 'FUNCTION' or type = 'PACKAGE') and name=upper('$(functionName)')) or (name||'_'||type=upper('$(functionName)')))" ;
    
    static final String SQL_DROP_FUNCTION = "DROP FUNCTION $(functionName)";
    
    public OracleSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new OracleTriggerTemplate(this);
        try {
            areDatabaseTransactionsPendingSince(System.currentTimeMillis());
            supportsTransactionViews = true;
        } catch (Exception ex) {
            if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW)) {
                log.warn("Was not able to enable the use of transaction views.  You might not have access to select from gv$transaction", ex);
            }
        }
    }
    
    @Override
    protected void buildSqlReplacementTokens() {
        super.buildSqlReplacementTokens();
        if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_HINTS,  true)) {
            sqlReplacementTokens.put("selectDataUsingGapsSqlHint", "/*+ index(d " + parameterService.getTablePrefix() + "_IDX_D_CHANNEL_ID) */");
        }
    }
    
    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        return platform.getSqlTemplate().queryForInt("select count(*) " + SQL_SELECT_TRIGGERS,
                new Object[] { triggerName, tableName }) > 0;                
    }    
    
    @Override
    protected String getDropTriggerSql(StringBuilder sqlBuffer, String catalogName,
            String schemaName, String triggerName, String tableName) {
        return "drop trigger " + triggerName;
    }    

    @Override
    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table) {
        try {
            super.createTrigger(sqlBuffer, dml, trigger, history, channel,
                    parameterService.getTablePrefix(), table);
        } catch (SqlException ex) {
            if (ex.getErrorCode() == 4095) {
                try {
                    // a trigger of the same name must already exist on a table
                    log.warn(
                            "TriggerAlreadyExists",
                            platform.getSqlTemplate().queryForMap(
                                    "select * " + SQL_SELECT_TRIGGERS,
                                    new Object[] { history.getTriggerNameForDmlType(dml),
                                            history.getSourceTableName() }));
                } catch (SqlException e) {
                }
            }
            throw ex;
        }
    }
    
    @Override
    public void createRequiredDatabaseObjects() {
        String blobToClob = this.parameterService.getTablePrefix() + "_" + "blob2clob";
        if (!installed(SQL_OBJECT_INSTALLED, blobToClob)) {
            String sql = "CREATE OR REPLACE FUNCTION $(functionName) (blob_in IN BLOB)                                                                                                                                           "
                    + "     RETURN CLOB                                                                                                                                                          "
                    + "   AS                                                                                                                                                                     "
                    + "       v_clob    CLOB := null;                                                                                                                                            "
                    + "       v_varchar VARCHAR2(32767);                                                                                                                                         "
                    + "       v_start   PLS_INTEGER := 1;                                                                                                                                        "
                    + "       v_buffer  PLS_INTEGER := 999;                                                                                                                                      "
                    + "   BEGIN                                                                                                                                                                  "
                    + "       IF blob_in IS NOT NULL THEN                                                                                                                                        "
                    + "           IF DBMS_LOB.GETLENGTH(blob_in) > 0 THEN                                                                                                                        "
                    + "               DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);                                                                                                                    "
                    + "               FOR i IN 1..CEIL(DBMS_LOB.GETLENGTH(blob_in) / v_buffer)                                                                                                   "
                    + "               LOOP                                                                                                                                                       "
                    + "                   v_varchar := UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.base64_encode(DBMS_LOB.SUBSTR(blob_in, v_buffer, v_start)));                                          "
                    + "                   v_varchar := REPLACE(v_varchar,CHR(13)||CHR(10));                                                                                                      "
                    + "                   DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_varchar), v_varchar);                                                                                            "
                    + "                   v_start := v_start + v_buffer;                                                                                                                         "
                    + "               END LOOP;                                                                                                                                                  "
                    + "           END IF;                                                                                                                                                        "
                    + "       END IF;                                                                                                                                                            "
                    + "       RETURN v_clob;                                                                                                                                                     "
                    + "   END $(functionName);                                                                                                                                                   ";
            install(sql, blobToClob);
        }

        String transactionId = this.parameterService.getTablePrefix() + "_" + "transaction_id";
        if (!installed(SQL_OBJECT_INSTALLED, transactionId)) {
            String sql = "CREATE OR REPLACE function $(functionName)                                                                                                                                                             "
                    + "   return varchar is                                                                                                                                                  "
                    + "   begin                                                                                                                                                              "
                    + "      return DBMS_TRANSACTION.local_transaction_id(false);                                                                                                            "
                    + "   end;                                                                                                                                                               ";
            install(sql, transactionId);
        }

        String triggerDisabled = this.parameterService.getTablePrefix() + "_" + "trigger_disabled";
        if (!installed(SQL_OBJECT_INSTALLED, triggerDisabled)) {
            String sql = "CREATE OR REPLACE function $(functionName) return varchar is                                                                                                                                           "
                    + "   begin                                                                                                                                                                "
                    + "      return "+getSymmetricPackageName()+".disable_trigger;                                                                                                                                   "
                    + "   end;                                                                                                                                                                 ";
            install(sql, triggerDisabled);
        }

        String pkgPackage = this.parameterService.getTablePrefix() + "_" + "pkg";
        if (!installed(SQL_OBJECT_INSTALLED, pkgPackage)) {
            String sql = "CREATE OR REPLACE package $(functionName) as                                                                                                                                                                   "
                    + "      disable_trigger pls_integer;                                                                                                                                       "
                    + "      disable_node_id varchar(50);                                                                                                                                       "
                    + "      procedure setValue (a IN number);                                                                                                                                  "
                    + "      procedure setNodeValue (node_id IN varchar);                                                                                                                       "
                    + "  end "+getSymmetricPackageName()+";                                                                                                                                                           ";
            install(sql, pkgPackage);
            
            sql = "CREATE OR REPLACE package body $(functionName) as                                                                                                                                                              "
                    + "     procedure setValue(a IN number) is                                                                                                                                 "
                    + "     begin                                                                                                                                                              "
                    + "         $(functionName).disable_trigger:=a;                                                                                                                                   "
                    + "     end;                                                                                                                                                               "
                    + "     procedure setNodeValue(node_id IN varchar) is                                                                                                                      "
                    + "     begin                                                                                                                                                              "
                    + "         $(functionName).disable_node_id := node_id;                                                                                                                           "
                    + "     end;                                                                                                                                                               "
                    + " end "+getSymmetricPackageName()+";                                                                                                                                                           ";
            install(sql, pkgPackage);
        }

        String wkt2geom = this.parameterService.getTablePrefix() + "_" + "wkt2geom";
        if (!installed(SQL_OBJECT_INSTALLED, wkt2geom)) {
            String sql = "  CREATE OR REPLACE                                                                                                         "
                    + "    FUNCTION $(functionName)(                            "
                    + "        clob_in IN CLOB)                                 "
                    + "      RETURN SDO_GEOMETRY                                "
                    + "    AS                                                   "
                    + "      v_out SDO_GEOMETRY := NULL;                        "
                    + "    BEGIN                                                "
                    + "      IF clob_in IS NOT NULL THEN                        "
                    + "        IF DBMS_LOB.GETLENGTH(clob_in) > 0 THEN          "
                    + "          v_out := SDO_GEOMETRY(clob_in);                "
                    + "        END IF;                                          "
                    + "      END IF;                                            "
                    + "      RETURN v_out;                                      "
                    + "    END $(functionName);                                 ";
            install(sql, wkt2geom);
        }
    }

    @Override
    public void dropRequiredDatabaseObjects() {
        String blobToClob = this.parameterService.getTablePrefix() + "_" + "blob2clob";
        if (installed(SQL_OBJECT_INSTALLED, blobToClob)) {
            uninstall(SQL_DROP_FUNCTION, blobToClob);
        }

        String transactionId = this.parameterService.getTablePrefix() + "_" + "transaction_id";
        if (installed(SQL_OBJECT_INSTALLED, transactionId)) {
            uninstall(SQL_DROP_FUNCTION, transactionId);
        }

        String triggerDisabled = this.parameterService.getTablePrefix() + "_" + "trigger_disabled";
        if (installed(SQL_OBJECT_INSTALLED, triggerDisabled)) {
            uninstall(SQL_DROP_FUNCTION, triggerDisabled);
        }
        
        String wkt2geom = this.parameterService.getTablePrefix() + "_" + "wkt2geom";
        if (installed(SQL_OBJECT_INSTALLED, wkt2geom)) {
            uninstall(SQL_DROP_FUNCTION, wkt2geom);
        }        

        String pkgPackage = this.parameterService.getTablePrefix() + "_" + "pkg";
        if (installed(SQL_OBJECT_INSTALLED, pkgPackage)) {
            uninstall("DROP PACKAGE $(functionName)", pkgPackage);
        }

    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return parameterService.getTablePrefix() + "_" + "transaction_id()";
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    public String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
        case REQUEST:
            return "SEQ_" + parameterService.getTablePrefix() + "_EXTRACT_EST_REQUEST_ID";
        case DATA:
            return "SEQ_" + parameterService.getTablePrefix() + "_DATA_DATA_ID";
        case TRIGGER_HIST:
            return "SEQ_" + parameterService.getTablePrefix() + "_TRIGGER_RIGGER_HIST_ID";
        }
        return null;
    }

    public void cleanDatabase() {
        platform.getSqlTemplate().update("purge recyclebin");
    }

    protected String getSymmetricPackageName() {
        return parameterService.getTablePrefix() + "_pkg";
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute(String.format("call %s.setValue(1)", getSymmetricPackageName()));
        if (nodeId != null) {
            transaction.prepareAndExecute(String.format("call %s.setNodeValue('" + nodeId + "')",
                    getSymmetricPackageName()));
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute(String.format("call %s.setValue(null)", getSymmetricPackageName()));
        transaction.prepareAndExecute(String.format("call %s.setNodeValue(null)", getSymmetricPackageName()));
    }

    public String getSyncTriggersExpression() {
        return parameterService.getTablePrefix() + "_trigger_disabled() is null";
    }

    @Override
    public boolean areDatabaseTransactionsPendingSince(long time) {
        String returnValue = platform.getSqlTemplate().queryForObject(SQL_SELECT_TRANSACTIONS,
                String.class);
        if (returnValue != null) {
            Date date;
            try {
                date = DateUtils.parseDate(returnValue, new String[] { "MM/dd/yy HH:mm:ss" });
                return date.getTime() < time;
            } catch (ParseException e) {
                log.error("", e);
                return true;
            }
        } else {
            return false;
        }

    }

    @Override
    public Date getEarliestTransactionStartTime() {
        Date date = null;
        String returnValue = platform.getSqlTemplate().queryForObject(SQL_SELECT_TRANSACTIONS, String.class);
        if (returnValue != null) {
            try {
                date = DateUtils.parseDate(returnValue, new String[] { "MM/dd/yy HH:mm:ss" });
            } catch (ParseException e) {
                log.error("", e);
                date = new Date();
            }
        }
        return date;
    }

    @Override
    public boolean supportsTransactionViews() {
        return supportsTransactionViews
                && parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW);
    }

    @Override
    public String massageDataExtractionSql(String sql, Channel channel) {
        if (channel != null && !channel.isContainsBigLob()) {
            sql = StringUtils.replace(sql, "d.row_data", "dbms_lob.substr(d.row_data, 4000, 1 )");
            sql = StringUtils.replace(sql, "d.old_data", "dbms_lob.substr(d.old_data, 4000, 1 )");
            sql = StringUtils.replace(sql, "d.pk_data", "dbms_lob.substr(d.pk_data, 4000, 1 )");
        }
        sql = super.massageDataExtractionSql(sql, channel);
        return sql;
    }

    @Override
    public String massageForLob(String sql, Channel channel) {
        if (channel != null && !channel.isContainsBigLob()) {
            return String.format("dbms_lob.substr(%s, 4000, 1)", sql);
        } else {
            return super.massageForLob(sql, channel);
        }
    }

    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        if (!trigger.isUseCaptureLobs()) {
            return "var_old_data is null or var_row_data != var_old_data";
        } else {
            return "dbms_lob.compare(nvl(var_row_data,'Null'),nvl(var_old_data,'Null')) != 0 ";
        }
    }

    public String getTemplateNumberPrecisionSpec() {
        return parameterService.getString(ParameterConstants.DBDIALECT_ORACLE_TEMPLATE_NUMBER_SPEC,"30,10");
    }
}
