package org.jumpmind.symmetric.db.oracle;

import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.db.AbstractDbDialect;
import org.jumpmind.symmetric.db.IDbDialect;
import org.jumpmind.symmetric.db.SqlScript;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class OracleDbDialect extends AbstractDbDialect implements IDbDialect {

    static final Log logger = LogFactory.getLog(OracleDbDialect.class);

    static final String TRANSACTION_ID_FUNCTION_NAME = "fn_transaction_id";

    static final String ORACLE_OBJECT_TYPE = "FUNCTION";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    @Override
    protected void initForSpecificDialect() {
        try {
            if (!isFunctionUpToDate(TRANSACTION_ID_FUNCTION_NAME)) {
                logger
                        .info("Creating function "
                                + TRANSACTION_ID_FUNCTION_NAME);
                new SqlScript(getTransactionIdSqlUrl(), getPlatform()
                        .getDataSource(), '/').execute();
            }
        } catch (Exception ex) {
            logger.error("Error while initializing Oracle.", ex);
        }

    }

    @Override
    public void initTrigger(DataEventType dml, Trigger trigger,
            TriggerHistory audit, String tablePrefix, Table table) {
        // TODO: fix node table trigger which cannot select itself
        if (!isSkipTriggerCreation(trigger.getSourceTableName())) {
            super.initTrigger(dml, trigger, audit, tablePrefix, table);
        } else {
            logger
                    .warn("Not creating trigger for "
                            + trigger.getSourceTableName()
                            + " because of a current bug we have with the oracle triggers and a trigger not being able to select from the table it fired for.");
        }
    }

    private boolean isSkipTriggerCreation(String table) {
        return table.toLowerCase().endsWith("node");
    }

    private URL getTransactionIdSqlUrl() {
        return getClass().getResource("/oracle-transactionid.sql");
    }

    public boolean isFunctionUpToDate(String name) throws Exception {
        long lastModified = getTransactionIdSqlUrl().openConnection()
                .getLastModified();

        SimpleDateFormat dateFormat = new SimpleDateFormat(
                "yyyy-MM-dd':'HH:mm:ss");

        return jdbcTemplate
                .queryForInt(
                        "select count(*) from all_objects where timestamp < ? and object_name= upper(?) ",
                        new Object[] {
                                dateFormat.format(new Date(lastModified)), name }) > 0;
    }

    public boolean isCharSpacePadded() {
        return true;
    }

    public boolean isCharSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return true;
    }

    public void removeTrigger(String schemaName, String triggerName) {
        schemaName = schemaName == null ? "" : (schemaName + ".");
        try {
            jdbcTemplate.update("drop trigger " + schemaName + triggerName);
        } catch (Exception e) {
            logger.warn("Trigger does not exist");
        }
    }

    public String getTransactionTriggerExpression() {
        return TRANSACTION_ID_FUNCTION_NAME + "()";
    }

    @Override
    public boolean doesTriggerExist(String schema, String tableName,
            String triggerName) {
        if (!isSkipTriggerCreation(tableName)) {
            return jdbcTemplate
                    .queryForInt(
                            "select count(*) from ALL_TRIGGERS  where trigger_name like upper(?) and table_name like upper(?)",
                            new Object[] { triggerName, tableName }) > 0;
        } else {
            return true;
        }
    }

    public void purge() {
        jdbcTemplate.update("purge recyclebin");

    }

    public void disableSyncTriggers() {
        jdbcTemplate.update("call  pack_var.setValue(1)");
    }

    public void enableSyncTriggers() {
        jdbcTemplate.update("call  pack_var.setValue(null)");
    }

    public String getDefaultSchema() {
        return (String) jdbcTemplate.queryForObject(
                "SELECT sys_context('USERENV', 'CURRENT_SCHEMA') FROM dual",
                String.class);
    }

}
