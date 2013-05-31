package org.jumpmind.symmetric.db.h2;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractEmbeddedSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.service.IParameterService;

/*
 * Synchronization support for the H2 database platform. 
 */
public class H2SymmetricDialect extends AbstractEmbeddedSymmetricDialect implements ISymmetricDialect {
    
    static final String SQL_DROP_FUNCTION = "DROP ALIAS $(functionName)";
    static final String SQL_FUNCTION_INSTALLED = "select count(*) from INFORMATION_SCHEMA.FUNCTION_ALIASES where ALIAS_NAME='$(functionName)'" ;

    public H2SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new H2TriggerTemplate(this);
    }
    
    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName, String tableName,
            String triggerName) {
        boolean exists = (platform.getSqlTemplate()
                .queryForInt("select count(*) from INFORMATION_SCHEMA.TRIGGERS WHERE TRIGGER_NAME = ?",
                        new Object[] { triggerName }) > 0)
                && (platform.getSqlTemplate().queryForInt("select count(*) from INFORMATION_SCHEMA.TABLES WHERE TABLE_NAME = ?",
                        new Object[] { String.format("%s_CONFIG", triggerName) }) > 0);

        if (!exists && !StringUtils.isBlank(triggerName)) {
            removeTrigger(new StringBuilder(), catalogName, schemaName, triggerName, tableName, null);
        }
        return exists;
    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        String prefix = Table.getQualifiedTablePrefix(catalogName, schemaName, getPlatform().getDatabaseInfo().getDelimiterToken());
        final String dropSql = String.format("DROP TRIGGER IF EXISTS %s%s", prefix, triggerName);
        logSql(dropSql, sqlBuffer);

        final String dropTable = String.format("DROP TABLE IF EXISTS %s%s_CONFIG", prefix, triggerName);
        logSql(dropTable, sqlBuffer);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
				log.debug(
						"Dropping trigger {} for {}",
						triggerName,
						oldHistory != null ? oldHistory
								.getFullyQualifiedSourceTableName() : Table
								.getFullyQualifiedTableName(catalogName,
										schemaName, tableName));
				platform.getSqlTemplate().update(dropSql);
                platform.getSqlTemplate().update(dropTable);
            } catch (Exception e) {
                log.warn("Error removing {}: {}", triggerName, e.getMessage());
            }
        }
    }
    
    @Override
    protected void createRequiredDatabaseObjects() {        
        String encode = this.parameterService.getTablePrefix().toUpperCase() + "_" + "BASE64_ENCODE";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
            String sql = "CREATE ALIAS IF NOT EXISTS $(functionName) for \"org.jumpmind.symmetric.db.EmbeddedDbFunctions.encodeBase64\"; ";
            install(sql, encode);
        }

    }
    
    @Override
    protected void dropRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix().toUpperCase() + "_" + "BASE64_ENCODE";
        if (installed(SQL_FUNCTION_INSTALLED, encode)) {
            uninstall(SQL_DROP_FUNCTION, encode);
        }
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public boolean isClobSyncSupported() {
        return true;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        transaction.prepareAndExecute("set @sync_prevented=1");
        transaction.prepareAndExecute("set @node_value=?", nodeId);
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("set @sync_prevented=null");
        transaction.prepareAndExecute("set @node_value=null");
    }

    public String getSyncTriggersExpression() {
        return " @sync_prevented is null ";
    }

    /*
     * An expression which the java trigger can string replace
     */
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "TRANSACTION_ID()";
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }


    @Override
    public boolean supportsTransactionId() {
        return true;
    }

}
