package org.jumpmind.symmetric.db.hsqldb;

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

public class HsqlDbSymmetricDialect extends AbstractEmbeddedSymmetricDialect implements ISymmetricDialect {

    static final String SQL_DROP_FUNCTION = "DROP ALIAS $(functionName)";
    static final String SQL_FUNCTION_INSTALLED = "select count(*) from INFORMATION_SCHEMA.SYSTEM_ALIASES where ALIAS='$(functionName)'" ;
    static final String  DUAL_TABLE = "DUAL";

    private boolean enforceStrictSize = true;    

    public HsqlDbSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new HsqlDbTriggerTemplate(this);

        platform.getSqlTemplate().update("SET WRITE_DELAY 100 MILLIS");
        platform.getSqlTemplate().update("SET PROPERTY \"hsqldb.default_table_type\" 'cached'");
        platform.getSqlTemplate().update("SET PROPERTY \"sql.enforce_strict_size\" " + enforceStrictSize);
        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                HsqlDbSymmetricDialect.this.platform.getSqlTemplate().update("SHUTDOWN");
            }
        });
        createDummyDualTable();        
    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schemaName, String tableName,
            String triggerName) {
        boolean exists = (platform.getSqlTemplate().queryForInt(
                "select count(*) from INFORMATION_SCHEMA.SYSTEM_TRIGGERS WHERE TRIGGER_NAME = ?",
                new Object[] { triggerName }) > 0)
                || (platform.getSqlTemplate().queryForInt(
                        "select count(*) from INFORMATION_SCHEMA.SYSTEM_TABLES WHERE TABLE_NAME = ?",
                        new Object[] { String.format("%s_CONFIG", triggerName) }) > 0);
        return exists;
    }
    
    @Override
    protected void createRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix() + "_" + "base_64_encode";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
            String sql = "CREATE ALIAS $(functionName) for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.encodeBase64\"; ";
            install(sql, encode);
        }

        String setSession = this.parameterService.getTablePrefix() + "_" + "set_session";
        if (!installed(SQL_FUNCTION_INSTALLED, setSession)) {
            String sql = "CREATE ALIAS $(functionName) for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.setSession\"; ";
            install(sql, setSession);
        }

        String getSession = this.parameterService.getTablePrefix() + "_" + "get_session";
        if (!installed(SQL_FUNCTION_INSTALLED, getSession)) {
            String sql = "CREATE ALIAS $(functionName) for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.getSession\"; ";
            install(sql, getSession);
        }
        
    }
    
    @Override
    protected void dropRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix() + "_" + "base_64_encode";
        if (installed(SQL_FUNCTION_INSTALLED, encode)) {
            uninstall(SQL_DROP_FUNCTION, encode);
        }

        String setSession = this.parameterService.getTablePrefix() + "_" + "set_session";
        if (installed(SQL_FUNCTION_INSTALLED, setSession)) {
            uninstall(SQL_DROP_FUNCTION, setSession);
        }

        String getSession = this.parameterService.getTablePrefix() + "_" + "get_session";
        if (installed(SQL_FUNCTION_INSTALLED, getSession)) {
            uninstall(SQL_DROP_FUNCTION, getSession);
        }

    }

    /*
     * This is for use in the java triggers so we can create a virtual table w/
     * old and new columns values to bump SQL expressions up against.
     */
    private void createDummyDualTable() {
        Table table = platform.getTableFromCache(null, null, DUAL_TABLE, true);
        if (table == null) {
            platform.getSqlTemplate().update("CREATE MEMORY TABLE " + DUAL_TABLE + "(DUMMY VARCHAR(1))");
            platform.getSqlTemplate().update("INSERT INTO " + DUAL_TABLE + " VALUES(NULL)");
            platform.getSqlTemplate().update("SET TABLE " + DUAL_TABLE + " READONLY TRUE");
        }

    }

    @Override
    public void removeTrigger(StringBuilder sqlBuffer, String catalogName, String schemaName, String triggerName,
            String tableName, TriggerHistory oldHistory) {
        final String dropSql = String.format("DROP TRIGGER %s", triggerName);
        logSql(dropSql, sqlBuffer);

        final String dropTable = String.format("DROP TABLE IF EXISTS %s_CONFIG", triggerName);
        logSql(dropTable, sqlBuffer);

        if (parameterService.is(ParameterConstants.AUTO_SYNC_TRIGGERS)) {
            try {
                int count = platform.getSqlTemplate().update(dropSql);
                if (count > 0) {
                    log.info("Just dropped trigger {}", triggerName);
                }
            } catch (Exception e) {
                log.warn("Error removing {}: {}", triggerName, e.getMessage());
            }
            try {
                int count = platform.getSqlTemplate().update(dropTable);
                if (count > 0) {
                    log.info("Just dropped table {}_CONFIG", triggerName);
                }
            } catch (Exception e) {
                log.warn("Error removing {}: {}", triggerName, e.getMessage());
            }
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
        transaction.prepareAndExecute("CALL " + parameterService.getTablePrefix() + "_set_session('sync_prevented','1')");
        transaction.prepareAndExecute("CALL " + parameterService.getTablePrefix() + "_set_session('node_value','"+nodeId+"')");
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("CALL " + parameterService.getTablePrefix() + "_set_session('sync_prevented',null)");
        transaction.prepareAndExecute("CALL " + parameterService.getTablePrefix() + "_set_session('node_value',null)");
    }

    public String getSyncTriggersExpression() {
        return " " + parameterService.getTablePrefix() + "_get_session(''sync_prevented'') is null ";
    }

    /*
     * An expression which the java trigger can string replace
     */
    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        // TODO Get I use a temporary table and a randomly generated GUID?
        return "null";
    }

   @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    public boolean isNonBlankCharColumnSpacePadded() {
        return enforceStrictSize;
    }

    public boolean isCharColumnSpaceTrimmed() {
        return false;
    }

    public boolean isEmptyStringNulled() {
        return false;
    }

    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    @Override
    public void truncateTable(String tableName) {
        platform.getSqlTemplate().update("delete from " + tableName);
    }

    @Override
    public boolean canGapsOccurInCapturedDataIds() {
        return false;
    }

}
