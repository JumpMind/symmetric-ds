package org.jumpmind.symmetric.db.mssql2000;

import org.jumpmind.db.platform.IDatabasePlatform;
import org.jumpmind.db.sql.ISqlTemplate;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.db.mssql.MsSqlSymmetricDialect;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.service.IParameterService;

public class MsSql2000SymmetricDialect extends MsSqlSymmetricDialect {

    
    public MsSql2000SymmetricDialect(IParameterService parameterService, IDatabasePlatform platform) {
        // Grandparent code called here because we can't call parent constructor.
        this.parameterService = parameterService;
        this.platform = platform;

        log.info("The DbDialect being used is {}", this.getClass().getName());

        buildSqlReplacementTokens();
        
        ISqlTemplate sqlTemplate = this.platform.getSqlTemplate();
        this.databaseMajorVersion = sqlTemplate.getDatabaseMajorVersion();
        this.databaseMinorVersion = sqlTemplate.getDatabaseMinorVersion();
        this.databaseName = sqlTemplate.getDatabaseProductName();
        this.databaseProductVersion = sqlTemplate.getDatabaseProductVersion();
        this.driverName = sqlTemplate.getDriverName();
        this.driverVersion = sqlTemplate.getDriverVersion();
        
        
        // Parent code
        this.triggerTemplate = new MsSql2000TriggerTemplate(this);
    }

    @Override
    public String getTransactionTriggerExpression(String defaultCatalog, String defaultSchema,
            Trigger trigger) {
        
        
//        String text = "if (@@TRANCOUNT > 0) " +
//        		"         begin\r\n" + 
//        		"             execute sp_getbindtoken @TransactionId output;\r\n" + 
//        		"         end";
        
        
        // Change TransactionId to TransactionIdExp to avoid duplicate var names.
//        String text = "declare @TransactionIdExp varchar;\r\n" + 
//                		"if (@@TRANCOUNT > 0) begin\r\n" + 
//                		" execute sp_getbindtoken @TransactionIdExp output;\r\n" + 
//                		"end\r\n" + 
//                		"select @TransactionIdExp";
        return "@TransactionId";
//        return text;
    }
    
    
    
    @Override
    protected void createRequiredDatabaseObjects() {
        // TODO: change name from base64 to hex
        String encode = this.parameterService.getTablePrefix() + "_" + "base64_encode";
        if (!installed(SQL_FUNCTION_INSTALLED, encode)) {
          String sql = "  create function dbo.$(functionName) (\r\n" + 
                  		"     @binvalue varbinary(255)) returns varchar(2000)\r\n" + 
                  		"   as \r\n" + 
                  		"   begin\r\n" + 
                  		"   declare @charvalue varchar(255)\r\n" + 
                  		"   declare @i int\r\n" + 
                  		"   declare @length int\r\n" + 
                  		"   declare @hexstring char(16)\r\n" + 
                  		"\r\n" + 
                  		"   select @charvalue = ''\r\n" + 
                  		"   select @i = 1\r\n" + 
                  		"   select @length = datalength(@binvalue)\r\n" + 
                  		"   select @hexstring = '0123456789abcdef'\r\n" + 
                  		"\r\n" + 
                  		"   while (@i <= @length)\r\n" + 
                  		"   begin\r\n" + 
                  		"\r\n" + 
                  		"     declare @tempint int\r\n" + 
                  		"     declare @firstint int\r\n" + 
                  		"     declare @secondint int\r\n" + 
                  		"\r\n" + 
                  		"     select @tempint = convert(int, substring(@binvalue,@i,1))\r\n" + 
                  		"     select @firstint = floor(@tempint/16)\r\n" + 
                  		"     select @secondint = @tempint - (@firstint*16)\r\n" + 
                  		"\r\n" + 
                  		"     select @charvalue = @charvalue +\r\n" + 
                  		"       substring(@hexstring, @firstint+1, 1) +\r\n" + 
                  		"       substring(@hexstring, @secondint+1, 1)\r\n" + 
                  		"\r\n" + 
                  		"     select @i = @i + 1\r\n" + 
                  		"   end\r\n" + 
                  		"    return @charvalue\r\n" + 
                  		"   end";
            install(sql, encode);
        }
        
//        SELECT @Context_Info = CONTEXT_INFO
//                FROM master.dbo.SYSPROCESSES 
//                WHERE SPID = @@SPID
        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
            String sql = 
                    "create function dbo.$(functionName)() returns smallint                                                                                                                                                 \r\n" + 
                    "   begin                                                                                                          \r\n" + 
                    "     declare @disabled varchar(1);\r\n" + 
                    "     declare @context_info varchar; \r\n" + 
                    "     SELECT @Context_Info = CONTEXT_INFO\r\n" + 
                    "        FROM master.dbo.SYSPROCESSES \r\n" + 
                    "        WHERE SPID = @@SPID                                                                                                                                   \r\n" + 
                    "     set @disabled = coalesce(replace(substring(cast(@context_info as varchar), 1, 1), 0x0, ''), '');                                                                     \r\n" + 
                    "     if @disabled is null or @disabled != '1'                                                                                                                              \r\n" + 
                    "       return 0;                                                                                                                                                           \r\n" + 
                    "     return 1;                                                                                                                                                             \r\n" + 
                    "   end  ";
            install(sql, triggersDisabled);
        }
//        String triggersDisabled = this.parameterService.getTablePrefix() + "_" + "triggers_disabled";
//        if (!installed(SQL_FUNCTION_INSTALLED, triggersDisabled)) {
//            String sql = "create function dbo.$(functionName)() returns smallint                                                                                                                                                 " + 
//                    "  begin                                                                                                                                                                  " + 
//                    "    declare @disabled varchar(1);                                                                                                                                        " + 
//                    "    set @disabled = coalesce(replace(substring(cast(context_info() as varchar), 1, 1), 0x0, ''), '');                                                                    " + 
//                    "    if @disabled is null or @disabled != '1'                                                                                                                             " + 
//                    "      return 0;                                                                                                                                                          " + 
//                    "    return 1;                                                                                                                                                            " + 
//                    "  end                                                                                                                                                                    ";
//            install(sql, triggersDisabled);
//        }
        
        
        
        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
            String sql = "create function dbo.$(functionName)() returns varchar(50)                                                                                                                                              " + 
                    "  begin                                                                                                                                                                  " + 
                    "    declare @node varchar(50);                                                                                                                                           " +
                    "     declare @context_info varchar; \r\n" + 
                    "     SELECT @Context_Info = CONTEXT_INFO\r\n" + 
                    "        FROM master.dbo.SYSPROCESSES \r\n" + 
                    "        WHERE SPID = @@SPID " +
                    "    set @node = coalesce(replace(substring(cast(@context_info as varchar) collate SQL_Latin1_General_CP1_CI_AS, 2, 50), 0x0, ''), '');                                  " + 
                    "    return @node;                                                                                                                                                        " + 
                    "  end                                                                                                                                                                    ";
            install(sql, nodeDisabled);
        }
//        String nodeDisabled = this.parameterService.getTablePrefix() + "_" + "node_disabled";
//        if (!installed(SQL_FUNCTION_INSTALLED, nodeDisabled)) {
//            String sql = "create function dbo.$(functionName)() returns varchar(50)                                                                                                                                              " + 
//                    "  begin                                                                                                                                                                  " + 
//                    "    declare @node varchar(50);                                                                                                                                           " + 
//                    "    set @node = coalesce(replace(substring(cast(context_info() as varchar) collate SQL_Latin1_General_CP1_CI_AS, 2, 50), 0x0, ''), '');                                  " + 
//                    "    return @node;                                                                                                                                                        " + 
//                    "  end                                                                                                                                                                    ";
//            install(sql, nodeDisabled);
//        }
        
    }
    
    @Override
    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.HEX;
    }
}
