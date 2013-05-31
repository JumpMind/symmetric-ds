package org.jumpmind.symmetric.db.db2;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * A dialect that is specific to DB2 databases
 */
public class Db2SymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    public Db2SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new Db2TriggerTemplate(this);
    }

    public boolean createOrAlterTablesIfNecessary() {
        boolean tablesCreated = super.createOrAlterTablesIfNecessary();
        if (tablesCreated) {
            long triggerHistId = platform.getSqlTemplate().queryForLong("select max(trigger_hist_id) from "
                    + parameterService.getTablePrefix() + "_trigger_hist") + 1;
            platform.getSqlTemplate().update("alter table " + parameterService.getTablePrefix()
                    + "_trigger_hist alter column trigger_hist_id restart with " + triggerHistId);
            log.info("Resetting auto increment columns for {}", parameterService.getTablePrefix() + "_trigger_hist");
            long dataId = platform.getSqlTemplate().queryForLong("select max(data_id) from " + parameterService.getTablePrefix()
                    + "_data") + 1;
            platform.getSqlTemplate().update("alter table " + parameterService.getTablePrefix()
                    + "_data alter column data_id restart with " + dataId);
            log.info("Resetting auto increment columns for {}", parameterService.getTablePrefix() + "_data");
        }
        return tablesCreated;
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalog, String schema, String tableName,
            String triggerName) {
        schema = schema == null ? (platform.getDefaultSchema() == null ? null : platform
                .getDefaultSchema()) : schema;
        return platform.getSqlTemplate().queryForInt(
                "select count(*) from syscat.triggers where trigname = ? and trigschema = ?",
                new Object[] { triggerName.toUpperCase(), schema.toUpperCase() }) > 0;
    }
    
    @Override
    protected void createRequiredDatabaseObjects() {        
    }
    
    @Override
    protected void dropRequiredDatabaseObjects() {
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
    }

    public String getSyncTriggersExpression() {
        return "1=1";
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        return "null";
    }

    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    public void purgeRecycleBin() {
    }

    @Override
    public void truncateTable(String tableName) {
        platform.getSqlTemplate().update("delete from " + tableName);
    }

    @Override
    protected String getDbSpecificDataHasChangedCondition(Trigger trigger) {
        return "var_old_data is null or var_row_data != var_old_data";
    }

}
