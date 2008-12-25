package org.jumpmind.symmetric.db.db2;

import java.net.URL;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.BinaryEncoding;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.Trigger;

public class Db2DbDialect extends AbstractDbDialect implements IDbDialect {

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    static final Log logger = LogFactory.getLog(Db2DbDialect.class);

    protected void initForSpecificDialect() {
        try {
            enableSyncTriggers();
        } catch (Exception e) {
            try {
                logger.info("Creating environment variables " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE
                        + " and " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE);
                new SqlScript(getSqlScriptUrl(), getPlatform().getDataSource(), ';').execute();
            } catch (Exception ex) {
                logger.error("Error while initializing DB2 dialect.", ex);
            }
        }
    }

    private URL getSqlScriptUrl() {
        return getClass().getResource("/dialects/db2.sql");
    }

    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
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

    public void removeTrigger(String catalogName, String schemaName, String triggerName,
            String tableName) {
        removeTrigger(schemaName, triggerName);
    }

    public boolean isBlobSyncSupported() {
        return false;
    }

    public boolean isClobSyncSupported() {
        return true;
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public void disableSyncTriggers(String nodeId) {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
        if (nodeId != null) {
            jdbcTemplate
                    .update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
        }
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        jdbcTemplate.update("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }

    public String getTransactionTriggerExpression(Trigger trigger) {
        return "nullif('','')";
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
        return false;
    }

    protected boolean allowsNullForIdentityColumn() {
        return false;
    }

    public void purge() {
    }

    public String getDefaultCatalog() {
        return null;
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject("values CURRENT SCHEMA", String.class);
    }

    public String getIdentifierQuoteString() {
        return "";
    }

}
