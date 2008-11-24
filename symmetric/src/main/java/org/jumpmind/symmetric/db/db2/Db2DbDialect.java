package org.jumpmind.symmetric.db.db2;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.model.Trigger;

public class Db2DbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(Db2DbDialect.class);

    protected void initForSpecificDialect() {
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        return true;
    }

    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName, String triggerName) {
        schema = schema == null ? (getDefaultSchema() == null ? null : getDefaultSchema()) : schema;
        return jdbcTemplate.queryForInt("select count(*) from syscat.triggers where trigname = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger " + triggerName + " does not exist");
        }
    }

    public void removeTrigger(String catalogName, String schemaName, String triggerName, String tableName) {
        removeTrigger(schemaName, triggerName);
    }

    public boolean isBlobSyncSupported() {
        // TODO:
        return false;
    }

    public boolean isClobSyncSupported() {
        return true;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public void disableSyncTriggers(String nodeId) {
        // TODO:
        //jdbcTemplate.queryForInt("values fn_sym_sync_triggers_set_disabled(1)");
        if (nodeId != null) {
            //jdbcTemplate.queryForObject("values fn_sym_sync_node_set_disabled('" + nodeId + "')", String.class);
        }
    }

    public void enableSyncTriggers() {
        // TODO:
        //jdbcTemplate.queryForInt("values fn_sym_sync_triggers_set_disabled(0)");
        //jdbcTemplate.queryForInt("values fn_sym_sync_node_set_disabled(null)");
    }

    public String getSyncTriggersExpression() {
        // TODO:
        //return "fn_sym_sync_triggers_disabled() = 0";
        return "1 = 1";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "fn_sym_transaction_id()";
    }

    public String getSelectLastInsertIdSql(String sequenceName) {
        return "values IDENTITY_VAL_LOCAL()";
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

    public boolean storesUpperCaseNamesInCatalog() {
        return true;
    }

    public boolean supportsGetGeneratedKeys() {
        // TODO:
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        // TODO:
        return true;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA", String.class);
    }
    
    public String getIdentifierQuoteString()
    {
        return "";
    }
}

