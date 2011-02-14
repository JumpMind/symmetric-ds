/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
 * <http://www.gnu.org/licenses/>.
 * 
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.  */


package org.jumpmind.symmetric.db.oracle;

import java.sql.Types;
import java.text.ParseException;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Table;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.lob.OracleLobHandler;
import org.springframework.jdbc.support.nativejdbc.NativeJdbcExtractor;

/**
 * A dialect that is specific to Oracle databases
 */
public class OracleDbDialect extends AbstractDbDialect implements IDbDialect {

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    String selectTriggerSql;
    
    String selectTransactionsSql;

    @Override
    public void init(Platform pf, int queryTimeout, JdbcTemplate jdbcTemplate) {
        super.init(pf, queryTimeout, jdbcTemplate);
        try {
            areDatabaseTransactionsPendingSince(System.currentTimeMillis());
            supportsTransactionViews = true;
        } catch (Exception ex) {
            if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW)) {
                log.warn(ex);
            }
        }
    }

    protected void initLobHandler() {
        lobHandler = new OracleLobHandler();
        try {
            Class<? extends NativeJdbcExtractor> clazz = Class.forName(parameterService
                    .getString(ParameterConstants.DB_NATIVE_EXTRACTOR)).asSubclass(NativeJdbcExtractor.class);
            NativeJdbcExtractor nativeJdbcExtractor = (NativeJdbcExtractor) clazz.newInstance();
            ((OracleLobHandler) lobHandler).setNativeJdbcExtractor(nativeJdbcExtractor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    protected Integer overrideJdbcTypeForColumn(Map<Object,Object> values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.startsWith("DATE")) {
            return Types.DATE;
        } else if (typeName != null && typeName.startsWith("TIMESTAMP")) {
            // This is for Oracle's TIMESTAMP(9)
            return Types.TIMESTAMP;
        } else if (typeName != null && typeName.startsWith("NVARCHAR")) {
            // This is for Oracle's NVARCHAR type
            return Types.VARCHAR;
        } else if (typeName != null && typeName.startsWith("NCHAR")) {
            return Types.CHAR;            
        } else if (typeName != null && typeName.startsWith("BINARY_FLOAT")) {
            return Types.FLOAT;
        } else if (typeName != null && typeName.startsWith("BINARY_DOUBLE")) {
            return Types.DOUBLE;
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
    }

    @Override
    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table) {
        try {
            super.createTrigger(sqlBuffer, dml, trigger, history, channel, tablePrefix, table);
        } catch (BadSqlGrammarException ex) {
            if (ex.getSQLException().getErrorCode() == 4095) {
                try {
                    // a trigger of the same name must already exist on a table
                    log.warn("TriggerAlreadyExists", jdbcTemplate.queryForMap(
                            "select * " + selectTriggerSql,
                            new Object[] { history.getTriggerNameForDmlType(dml), history.getSourceTableName() }));
                } catch (DataAccessException e) {
                }
            }
            throw ex;
        }
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public boolean isNonBlankCharColumnSpacePadded() {
        return true;
    }

    public boolean isCharColumnSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return true;
    }

    @Override
    public boolean isDateOverrideToTimestamp() {
        return true;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return tablePrefix + "_" + "transaction_id()";
    }

    @Override
    public boolean supportsTransactionId() {
        return true;
    }

    @Override
    protected String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
        case OUTGOING_BATCH:
            return "SEQ_SYM_OUTGOIN_BATCH_BATCH_ID";
        case DATA:
            return "SEQ_SYM_DATA_DATA_ID";
        case TRIGGER_HIST:
            return "SEQ_SYM_TRIGGER_RIGGER_HIST_ID";
        }
        return null;
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select " + sequenceName + ".currval from dual";
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        return jdbcTemplate
                .queryForInt(
                        "select count(*) " + selectTriggerSql,
                        new Object[] { triggerName, tableName }) > 0;
    }

    @Override
    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public void purge() {
        jdbcTemplate.update("purge recyclebin");
    }

    protected String getSymmetricPackageName() {
        return tablePrefix + "_pkg";
    }

    public void disableSyncTriggers(JdbcTemplate jdbcTemplate, String nodeId) {
        jdbcTemplate.update(String.format("call %s.setValue(1)", getSymmetricPackageName()));
        if (nodeId != null) {
            jdbcTemplate.update(String.format("call %s.setNodeValue('" + nodeId + "')",
                    getSymmetricPackageName()));
        }
    }

    public void enableSyncTriggers(JdbcTemplate jdbcTemplate) {
        jdbcTemplate.update(String.format("call %s.setValue(null)", getSymmetricPackageName()));
        jdbcTemplate.update(String.format("call %s.setNodeValue(null)", getSymmetricPackageName()));
    }

    public String getSyncTriggersExpression() {
        return tablePrefix + "_trigger_disabled() is null";
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            this.defaultSchema = (String) jdbcTemplate.queryForObject(
                    "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual", String.class);
        }
        return defaultSchema;
    }

    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {

    }

    @Override
    public boolean areDatabaseTransactionsPendingSince(long time) {
        String returnValue = jdbcTemplate.queryForObject(selectTransactionsSql, String.class);
        if (returnValue != null) {
            Date date;
            try {
                date = DateUtils.parseDate(returnValue, new String[] { "MM/dd/yy HH:mm:ss" });
                return date.getTime() < time;
            } catch (ParseException e) {
                log.error(e);
                return true;
            }
        } else {
            return false;
        }

    }

    public void setSelectTransactionsSql(String selectTransactionSql) {
        this.selectTransactionsSql = selectTransactionSql;
    }
    
    public void setSelectTriggerSql(String selectTriggerSql) {
        this.selectTriggerSql = selectTriggerSql;
    }
    
    @Override
    public boolean supportsTransactionViews() {
        return supportsTransactionViews && parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW);
    }
    
    @Override
    public String massageDataExtractionSql(String sql, Channel channel) {
        if (channel != null && !channel.isContainsBigLob()) {
            sql = StringUtils.replace(sql, "d.row_data", "dbms_lob.substr(d.row_data, 4000, 1 )");
            sql = StringUtils.replace(sql, "d.old_data", "dbms_lob.substr(d.old_data, 4000, 1 )");
            sql = StringUtils.replace(sql, "d.pk_data", "dbms_lob.substr(d.pk_data, 4000, 1 )");
        }
        return sql;        
    }
    
    public String massageLobColumn(String columnName, Channel channel) {   
        if (channel != null && !channel.isContainsBigLob()) {
            return String.format("dbms_lob.substr(%s, 4000, 1 )", columnName);
        } else {
            return columnName;
        }
    }
    
    @Override
    protected String getDbSpecificDataHasChangedCondition() {
        return "var_row_data != var_old_data";
    }
    
}