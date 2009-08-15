/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.db.postgresql;

import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.symmetric.SymmetricDSException;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.support.TransactionCallbackWithoutResult;

public class PostgreSqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final ILog log = LogFactory.getLog(PostgreSqlDbDialect.class);

    static final String TRANSACTION_ID_EXPRESSION = "txid_current()";

    static final String SYNC_TRIGGERS_DISABLED_VARIABLE = "symmetric.triggers_disabled";

    static final String SYNC_NODE_DISABLED_VARIABLE = "symmetric.node_disabled";

    private boolean supportsTransactionId = false;

    private String transactionIdExpression = "null";

    @Override
    protected void initForSpecificDialect() {
        if (getMajorVersion() >= 8 && getMinorVersion() >= 3) {
            log.info("TransactionIDSupportEnabling");
            supportsTransactionId = true;
            transactionIdExpression = TRANSACTION_ID_EXPRESSION;
        }
        try {
            enableSyncTriggers();
        } catch (Exception e) {
            log.error("PostgreSqlCustomVariableMissing");
            throw new SymmetricDSException("PostgreSqlCustomVariableMissing", e);
        }

    }

    @SuppressWarnings("unchecked")
    @Override
    protected Integer overrideJdbcTypeForColumn(Map values) {
        String typeName = (String) values.get("TYPE_NAME");
        if (typeName != null && typeName.equalsIgnoreCase("ABSTIME")) {
            return Types.TIMESTAMP;
        } else {
            return super.overrideJdbcTypeForColumn(values);
        }
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
        return "$(defaultSchema)fn_sym_triggers_disabled() = 0";
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

    public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
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
