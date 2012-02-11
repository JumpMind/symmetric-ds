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
import org.springframework.dao.DataAccessException;
import org.springframework.jdbc.BadSqlGrammarException;

/*
 * A dialect that is specific to Oracle databases
 */
public class OracleSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    String selectTriggerSql = "from ALL_TRIGGERS where owner in (SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual) and trigger_name like upper(?) and table_name like upper(?)";

    String selectTransactionsSql = "select min(start_time) from gv$transaction";

    public OracleSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerText = new OracleTriggerTemplate();
        try {
            areDatabaseTransactionsPendingSince(System.currentTimeMillis());
            supportsTransactionViews = true;
        } catch (Exception ex) {
            if (parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW)) {
                log.warn(ex.getMessage(),ex);
            }
        }
    }

    @Override
    public void createTrigger(StringBuilder sqlBuffer, DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table) {
        try {
            super.createTrigger(sqlBuffer, dml, trigger, history, channel,
                    parameterService.getTablePrefix(), table);
        } catch (BadSqlGrammarException ex) {
            if (ex.getSQLException().getErrorCode() == 4095) {
                try {
                    // a trigger of the same name must already exist on a table
                    log.warn(
                            "TriggerAlreadyExists",
                            platform.getSqlTemplate().queryForMap(
                                    "select * " + selectTriggerSql,
                                    new Object[] { history.getTriggerNameForDmlType(dml),
                                            history.getSourceTableName() }));
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
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        return platform.getSqlTemplate().queryForInt("select count(*) " + selectTriggerSql,
                new Object[] { triggerName, tableName }) > 0;
    }

    public void purge() {
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
        String returnValue = platform.getSqlTemplate().queryForObject(selectTransactionsSql,
                String.class);
        if (returnValue != null) {
            Date date;
            try {
                date = DateUtils.parseDate(returnValue, new String[] { "MM/dd/yy HH:mm:ss" });
                return date.getTime() < time;
            } catch (ParseException e) {
                log.error(e.getMessage(),e);
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
        return supportsTransactionViews
                && parameterService.is(ParameterConstants.DBDIALECT_ORACLE_USE_TRANSACTION_VIEW);
    }

    @Override
    public String massageDataExtractionSql(String sql, Channel channel) {
        if (channel != null && !channel.isContainsBigLob()) {
            sql = StringUtils.replace(sql, "d.row_data", "dbms_lob.substr(d.row_data, 4000, 1 ) as row_data");
            sql = StringUtils.replace(sql, "d.old_data", "dbms_lob.substr(d.old_data, 4000, 1 ) as old_data");
            sql = StringUtils.replace(sql, "d.pk_data", "dbms_lob.substr(d.pk_data, 4000, 1 ) as pk_data");
        }
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
            return "var_row_data != var_old_data";
        } else {
            return "dbms_lob.compare(nvl(var_row_data,'Null'),nvl(var_old_data,'Null')) != 0 ";
        }
    }

}
