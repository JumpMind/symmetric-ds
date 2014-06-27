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


package org.jumpmind.symmetric.db.postgresql;

import java.sql.Types;
import java.util.Map;

import javax.sql.rowset.serial.SerialBlob;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.SymmetricException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.ddl.Platform;
import org.jumpmind.symmetric.ddl.model.Column;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

/**
 * 
 */
public class PostgreSqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final String TRANSACTION_ID_EXPRESSION = "txid_current()";

    static final String SYNC_TRIGGERS_DISABLED_VARIABLE = "symmetric.triggers_disabled";

    static final String SYNC_NODE_DISABLED_VARIABLE = "symmetric.node_disabled";

    private boolean supportsTransactionId = false;

    private String transactionIdExpression = "null";

    @Override
    public void init(Platform pf, int queryTimeout, JdbcTemplate jdbcTemplate) {        
        super.init(pf, 0, jdbcTemplate);
    }
    
    @Override
    protected void initTablesAndFunctionsForSpecificDialect() {
        if (getMajorVersion() >= 8 && getMinorVersion() >= 3) {
            log.info("TransactionIDSupportEnabling");
            supportsTransactionId = true;
            transactionIdExpression = TRANSACTION_ID_EXPRESSION;
        }
        try {
            enableSyncTriggers();
        } catch (Exception e) {
            log.error("PostgreSqlCustomVariableMissing");
            throw new SymmetricException("PostgreSqlCustomVariableMissing", e);
        }

    }

    @Override
    public boolean requiresAutoCommitFalseToSetFetchSize() {
        return true;
    }

    @SuppressWarnings("unchecked")
    @Override
    protected Integer overrideJdbcTypeForColumn(Map values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.equalsIgnoreCase("ABSTIME")) {
            return Types.TIMESTAMP;
        } else if (typeName != null && typeName.equalsIgnoreCase("OID")) {
            return Types.BLOB;
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
    }
    
    @Override
    public Object[] getObjectValues(BinaryEncoding encoding, String[] values,
        Column[] orderedMetaData) {

        Object[] objectValues = super.getObjectValues(encoding, values, orderedMetaData);
        for (int i = 0; i < orderedMetaData.length; i++) {
            if (orderedMetaData[i] != null && orderedMetaData[i].getTypeCode() == Types.BLOB) {
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
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt("select count(*) from information_schema.triggers where trigger_name = ? "
                + "and event_object_table = ? and trigger_schema = ?", new Object[] { triggerName.toLowerCase(),
                tableName.toLowerCase(), schema == null ? defaultSchema : schema }) > 0;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        final String dropSql = "drop trigger " + triggerName + " on " + schemaName + tableName;
        logSql(dropSql, sqlBuffer);
        final String dropFunction = "drop function " + schemaName + "f" + triggerName + "()";
        logSql(dropFunction, sqlBuffer);
        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                jdbcTemplate.update(dropSql);
                jdbcTemplate.update(dropFunction);
            } catch (Exception e) {
                log.warn("TriggerDoesNotExist");
            }
        }
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.queryForList("select set_config('" + SYNC_TRIGGERS_DISABLED_VARIABLE + "', '1', false)");
        if (nodeId == null) {
            nodeId = "";
        }
        jdbcTemplate.queryForList("select set_config('" + SYNC_NODE_DISABLED_VARIABLE + "', '" + nodeId + "', false)");
    }

    public void enableSyncTriggers() {
        transactionTemplate.execute(new TransactionCallbackWithoutResult() {
            protected void doInTransactionWithoutResult(TransactionStatus transactionstatus) {
                if (!transactionstatus.isRollbackOnly()) {
                    jdbcTemplate
                            .queryForList("select set_config('" + SYNC_TRIGGERS_DISABLED_VARIABLE + "', '', false)");
                    jdbcTemplate.queryForList("select set_config('" + SYNC_NODE_DISABLED_VARIABLE + "', '', false)");
                }
            }
        });
    }

    public String getSyncTriggersExpression() {
        return "$(defaultSchema)" + tablePrefix + "_triggers_disabled() = 0";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return transactionIdExpression;
    }

    @Override
    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select currval('" + sequenceName + "_seq')";
    }

    @Override
    public boolean requiresSavepointForFallback() {
        return true;
    }

    public boolean isNonBlankCharColumnSpacePadded() {
        return true;
    }

    public boolean isCharColumnSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    @Override
    public boolean storesLowerCaseNamesInCatalog() {
        return true;
    }

    @Override
    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    @Override
    public boolean supportsTransactionId() {
        return supportsTransactionId;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    @Override
    public String getDefaultSchema() {
        if (StringUtils.isBlank(this.defaultSchema)) {
            defaultSchema = (String) jdbcTemplate.queryForObject("select current_schema()", String.class);
        }
        return super.getDefaultSchema();
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

}