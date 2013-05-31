package org.jumpmind.symmetric.db.interbase;

import java.util.List;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.mapper.StringMapper;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.SequenceIdentifier;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;
import org.springframework.jdbc.UncategorizedSQLException;

/*
 * Database dialect for <a href="http://www.embarcadero.com/products/interbase/">Interbase</a>.
 */
public class InterbaseSymmetricDialect extends AbstractSymmetricDialect implements ISymmetricDialect {

    public static final String CONTEXT_TABLE_NAME = "context";

    static final String CONTEXT_TABLE_CREATE = "create global temporary table %s (id varchar(30), context_value varchar(30)) on commit preserve rows";

    static final String CONTEXT_TABLE_INSERT = "insert into %s (id, context_value) values (?, ?)";

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";
    
    static final String SQL_FUNCTION_INSTALLED = "select count(*) from rdb$functions where rdb$function_name = upper('$(functionName)')" ;

    static final String SQL_DROP_FUNCTION = "DROP EXTERNAL FUNCTION $(functionName)";

    public InterbaseSymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new InterbaseTriggerTemplate(this);
    }
    
    @Override
    protected void createRequiredDatabaseObjects() {
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        try {
            platform.getSqlTemplate().queryForInt("select count(*) from " + contextTableName);
        } catch (Exception e) {
            try {
                log.info("Creating global temporary table {}", contextTableName);
                platform.getSqlTemplate().update(String.format(CONTEXT_TABLE_CREATE, contextTableName));
            } catch (Exception ex) {
                log.error("Error while initializing Interbase dialect", ex);
            }
        }
        
        String escape = this.parameterService.getTablePrefix() + "_" + "escape";
        if (!installed(SQL_FUNCTION_INSTALLED, escape)) {
            String sql = "declare external function $(functionName) cstring(32660)                                                                                                                                               " + 
                    "  returns cstring(32660) free_it entry_point 'sym_escape' module_name 'sym_udf'                                                                                          ";
            install(sql, escape);
        }
        
        String hex = this.parameterService.getTablePrefix() + "_" + "hex";
        if (!installed(SQL_FUNCTION_INSTALLED, hex)) {
            String sql = "declare external function $(functionName) blob                                                                                                                                                         " + 
                    "  returns cstring(32660) free_it entry_point 'sym_hex' module_name 'sym_udf'                                                                                             ";
            install(sql, hex);
        }
        
        String rtrim = this.parameterService.getTablePrefix() + "_" + "rtrim";
        if (!installed(SQL_FUNCTION_INSTALLED, rtrim)) {
            String sql = "declare external function $(functionName) cstring(32767)                                                                                                                                               " + 
                    "                                returns cstring(32767) free_it entry_point 'IB_UDF_rtrim' module_name 'ib_udf'                                                                                         ";
            install(sql, rtrim);
        }        

        try {
            platform.getSqlTemplate().queryForObject("select sym_escape('') from rdb$database", String.class);
        } catch (UncategorizedSQLException e) {
            if (e.getSQLException().getErrorCode() == -804) {
                log.error("Please install the sym_udf.so/dll to your {interbase_home}/UDF folder");
            }
            throw new RuntimeException("Function SYM_ESCAPE is not installed", e);
        }
    }
    
    @Override
    protected void dropRequiredDatabaseObjects() {
        String escape = this.parameterService.getTablePrefix() + "_" + "escape";
        if (installed(SQL_FUNCTION_INSTALLED, escape)) {
            uninstall(SQL_DROP_FUNCTION, escape);
        }
        
        String hex = this.parameterService.getTablePrefix() + "_" + "hex";
        if (installed(SQL_FUNCTION_INSTALLED, hex)) {
            uninstall(SQL_DROP_FUNCTION, hex);
        }
        
        String rtrim = this.parameterService.getTablePrefix() + "_" + "rtrim";
        if (installed(SQL_FUNCTION_INSTALLED, rtrim)) {
            uninstall(SQL_DROP_FUNCTION, rtrim);
        }        

    }

    @Override
    protected boolean doesTriggerExistOnPlatform(String catalogName, String schema, String tableName, String triggerName) {
        return platform.getSqlTemplate().queryForInt("select count(*) from rdb$triggers where rdb$trigger_name = ?",
                new Object[] { triggerName.toUpperCase() }) > 0;
    }

    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        transaction.prepareAndExecute(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
            SYNC_TRIGGERS_DISABLED_USER_VARIABLE, "1" });
        if (nodeId != null) {
            transaction.prepareAndExecute(String.format(CONTEXT_TABLE_INSERT, contextTableName), new Object[] {
                SYNC_TRIGGERS_DISABLED_NODE_VARIABLE, nodeId });
        }
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        String contextTableName = parameterService.getTablePrefix() + "_" + CONTEXT_TABLE_NAME;
        transaction.prepareAndExecute("delete from " + contextTableName);
    }

    public String getSyncTriggersExpression() {
        return ":" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }
    
    @Override
    public String getSequenceName(SequenceIdentifier identifier) {
        switch (identifier) {
        case DATA:
            return "SYM_DATA_DATA_ID";
        case TRIGGER_HIST:
            return "SYM_TRIGGER_TRIGGER_HIST_ID";
        }
        return null;
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema, Trigger trigger) {
        return "null";
    }

    @Override
    public boolean isBlobSyncSupported() {
        return true;
    }

    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }

    public void purgeRecycleBin() {
    }

    @Override
    public String getName() {
        return super.getName();
    }

    @Override
    public boolean supportsOpenCursorsAcrossCommit() {
        return false;
    }
    
    @Override
    public boolean supportsTransactionId() {
        return false;
    }

    @Override
    public boolean supportsBatchUpdates() {
        // Interbase and interclient driver do have support for batch updates, 
        // but we get primary/unique key violation when enabling its use
        return false;
    }

    @Override
    public void truncateTable(String tableName) {
        platform.getSqlTemplate().update("delete from " + tableName);
    }
        
    @Override
    public void cleanupTriggers() {
        List<String> names = platform.getSqlTemplate().query("select rdb$trigger_name from rdb$triggers where rdb$trigger_name like '"+parameterService.getTablePrefix().toUpperCase()+"_%'", new StringMapper());
        for (String name : names) {
            platform.getSqlTemplate().update("drop trigger " + name);
            log.info("Dropped trigger {}", name);
        }
    }
}
