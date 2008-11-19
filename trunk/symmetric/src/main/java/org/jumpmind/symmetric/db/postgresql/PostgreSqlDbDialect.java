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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class PostgreSqlDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(PostgreSqlDbDialect.class);

    static final String TRANSACTION_ID_EXPRESSION = "txid_current()";

    static final String SYNC_TRIGGERS_DISABLED_VARIABLE = "symmetric.triggers_disabled";
    
    static final String SYNC_NODE_DISABLED_VARIABLE = "symmetric.node_disabled";

    private boolean supportsTransactionId = false;

    private String transactionIdExpression = "null";

    protected String defaultSchema;

    protected void initForSpecificDialect() {
        if (getMajorVersion() >= 8 && getMinorVersion() >= 3) {
            logger.info("Enabling transaction ID support");
            supportsTransactionId = true;
            transactionIdExpression = TRANSACTION_ID_EXPRESSION;
        }
        try {
            enableSyncTriggers();
        } catch (Exception e) {
            logger.error("Please add \"custom_variable_classes = 'symmetric'\" to your postgresql.conf file");
            throw new RuntimeException("Missing custom variable class 'symmetric'", e);
        }
        defaultSchema = (String) jdbcTemplate.queryForObject("select current_schema()", String.class);        
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return jdbcTemplate.queryForInt(
            "select count(*) from information_schema.triggers where trigger_name = ? " +
            "and event_object_table = ? and trigger_schema = ?",
            new Object[] { triggerName.toLowerCase(), tableName.toLowerCase(), schema == null ? defaultSchema : schema }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        throw new RuntimeException("Not implemented.  Use removeTrigger(schema, trigger, table) instead.");
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + triggerName + " on " + schemaName + tableName);
            jdbcTemplate.update("drop function " + schemaName + "f" + triggerName + "()");
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
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
        jdbcTemplate.queryForList("select set_config('" + SYNC_TRIGGERS_DISABLED_VARIABLE + "', '', false)");
        jdbcTemplate.queryForList("select set_config('" + SYNC_NODE_DISABLED_VARIABLE + "', '', false)");
    }

    public String getSyncTriggersExpression() {
        return "fn_sym_triggers_disabled() = 0";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return transactionIdExpression;
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "select currval('" + sequenceName + "_seq')";
    }

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

    public boolean storesLowerCaseNamesInCatalog() {
        return true;
    }

    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public boolean supportsTransactionId() {
        return supportsTransactionId;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return defaultSchema;
    }
    
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

}
