package org.jumpmind.symmetric.db.mssql;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTransaction;
import org.jumpmind.symmetric.service.IParameterService;

public class MsSql2016SymmetricDialect extends MsSql2008SymmetricDialect {
    
	static final String SYNC_TRIGGERS_DISABLED_USER_VARIABLE = "@sync_triggers_disabled";

    static final String SYNC_TRIGGERS_DISABLED_NODE_VARIABLE = "@sync_node_disabled";

    public MsSql2016SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        super(parameterService, platform);
        this.triggerTemplate = new MsSql2016TriggerTemplate(this);
    }
    
    @Override
    public void createRequiredDatabaseObjects() {
        String encode = this.parameterService.getTablePrefix() + "_" + "base64_encode";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
            String sql = "create function dbo.$(functionName)(@data varbinary(max)) returns varchar(max)                                                                                                                         " + 
                    "\n  with schemabinding, returns null on null input                                                                                                                       " + 
                    "\n  begin                                                                                                                                                                " + 
                    "\n    return ( select [text()] = @data for xml path('') )                                                                                                                " + 
                    "\n  end                                                                                                                                                                  ";
            install(sql, encode);
        }

        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            String sql = "create function dbo.$(functionName)() returns smallint                                                                                                                                                 " + 
                    "\n  begin                                                                                                                                                                  " + 
                    "\n    declare @disabled varchar(50);                                                                                                                                        " + 
                    "\n    set @disabled = CONVERT(varchar(50), SESSION_CONTEXT(N'" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "'));                                                                                                   " + 
                    "\n    if @disabled is null                                                                                                                              " + 
                    "\n      return 0;                                                                                                                                                          " + 
                    "\n    return 1;                                                                                                                                                            " + 
                    "\n  end                                                                                                                                                                    ";
            install(sql, triggersDisabled);
        }

        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            String sql = "create function dbo.$(functionName)() returns varchar(50)                                                                                                                                              " + 
                    "\n  begin                                                                                                                                                                  " + 
                    "\n    declare @node varchar(50);                                                                                                                                           " + 
                    "\n    set @node = CONVERT(varchar(50), SESSION_CONTEXT(N'" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "'));                                                                                                         " +
                    "\n    return @node;                                                                                                                                                        " + 
                    "\n  end                                                                                                                                                                    ";
            install(sql, nodeDisabled);
        }
       
    }
    
    public void disableSyncTriggers(ISqlTransaction transaction, String nodeId) {
    	transaction.prepareAndExecute("EXEC sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "', '1';");
        if (nodeId == null) {
            nodeId = "";
        }
        transaction.prepareAndExecute("EXEC sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "', '" + nodeId + "';");
    }

    public void enableSyncTriggers(ISqlTransaction transaction) {
        transaction.prepareAndExecute("EXEC sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_USER_VARIABLE + "', NULL;");
        transaction.prepareAndExecute("EXEC sp_set_session_context '" + SYNC_TRIGGERS_DISABLED_NODE_VARIABLE + "', NULL;");
    }
    
    

}
