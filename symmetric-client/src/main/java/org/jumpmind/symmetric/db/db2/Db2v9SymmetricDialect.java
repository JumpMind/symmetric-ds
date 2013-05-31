package org.jumpmind.symmetric.db.db2;

import java.net.URL;

import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.db.sql.SqlScript;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.service.IParameterService;

public class Db2v9SymmetricDialect extends Db2SymmetricDialect implements ISymmetricDialect {

    static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "sync_node_disabled";

    public Db2v9SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
    }
    
    @Override
    protected void createRequiredDatabaseObjects() {
        ISqlTransaction transaction = null;
        try {
            transaction = platform.getSqlTemplate().startSqlTransaction();
            enableSyncTriggers(transaction);
            transaction.commit();
        } catch (Exception e) {
            try {
                log.info("Creating environment variables {} and {}", SYNC_TRIGGERS_DISABLED_USER_VARIABLE,
                        SYNC_TRIGGERS_DISABLED_NODE_VARIABLE);
                new SqlScript(getSqlScriptUrl(), getPlatform().getSqlTemplate(), ";").execute();
            } catch (Exception ex) {
                log.error("Error while initializing DB2 dialect", ex);
            }
        } finally {
            close(transaction);
        }

        super.createRequiredDatabaseObjects();
    }

    private URL getSqlScriptUrl() {
        return getClass().getResource("/org/jumpmind/symmetric/db/db2.sql");
    }
    
    @Override
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
       transaction.execute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=1");
       if (StringUtils.isNotBlank(nodeId)) {
           transaction.execute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "='" + nodeId + "'");
       }
    }

    @Override
    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "=null");
        transaction.execute("set " + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "=null");
    }

    public String getSyncTriggersExpression() {
        return SYNC_TRIGGERS_DISABLED_USER_VARIABLE + " is null";
    }
    
    @Override
    public String getSourceNodeExpression() {
        return SYNC_TRIGGERS_DISABLED_NODE_VARIABLE;
    }
    
}
