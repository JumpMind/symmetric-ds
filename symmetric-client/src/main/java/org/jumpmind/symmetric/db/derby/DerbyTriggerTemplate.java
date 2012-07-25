package org.jumpmind.symmetric.db.derby;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class DerbyTriggerTemplate extends AbstractTriggerTemplate {

    public DerbyTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect); 
        functionInstalledSql = "select count(*) from sys.sysaliases where alias = upper('$(functionName)')" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "sym_escape($(tableAlias).\"$(columnName)\")" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || rtrim(char($(tableAlias).\"$(columnName)\")) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || rtrim(char($(tableAlias).\"$(columnName)\")) || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "sym_clob_to_string('\"$(columnName)\"', '$(schemaName)$(tableName)', $(primaryKeyWhereString) )" ;
        blobColumnTemplate = "sym_blob_to_string('\"$(columnName)\"', '$(schemaName)$(tableName)', $(primaryKeyWhereString) )" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = null;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = null;

        functionTemplatesToInstall = new HashMap<String,String>();
        functionTemplatesToInstall.put("escape" ,
"CREATE FUNCTION $(functionName)(STR VARCHAR(10000)) RETURNS                                                                                                                                            " + 
"                                VARCHAR(10000) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                 " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.escape'                                                                                                                " );
        functionTemplatesToInstall.put("transaction_id" ,
"CREATE FUNCTION $(functionName)() RETURNS                                                                                                                                                              " + 
"                                varchar(100) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                   " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.getTransactionId'                                                                                                      " );
        functionTemplatesToInstall.put("sync_triggers_disabled" ,
"CREATE FUNCTION $(functionName)() RETURNS                                                                                                                                                              " + 
"                                integer PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                        " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.isSyncDisabled'                                                                                                        " );
        functionTemplatesToInstall.put("sync_triggers_set_disabled" ,
"CREATE FUNCTION $(functionName)(state integer) RETURNS                                                                                                                                                 " + 
"                                integer PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                        " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.setSyncDisabled'                                                                                                       " );
        functionTemplatesToInstall.put("sync_node_set_disabled" ,
"CREATE FUNCTION $(functionName)(nodeId varchar(50)) RETURNS                                                                                                                                            " + 
"                                varchar(50) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                    " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.setSyncNodeDisabled'                                                                                                   " );
        functionTemplatesToInstall.put("clob_to_string" ,
"CREATE FUNCTION $(functionName)(columnName varchar(50),                                                                                                                                                " + 
"                                tableName varchar(50), whereClause varchar(8000)) RETURNS                                                                                                              " + 
"                                varchar(32672) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA EXTERNAL NAME                                                                                         " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.clobToString'                                                                                                          " );
        functionTemplatesToInstall.put("blob_to_string" ,
"CREATE FUNCTION $(functionName)(columnName varchar(50),                                                                                                                                                " + 
"                                tableName varchar(50), whereClause varchar(8000)) RETURNS                                                                                                              " + 
"                                varchar(32672) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA EXTERNAL NAME                                                                                         " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.blobToString'                                                                                                          " );
        functionTemplatesToInstall.put("insert_data" ,
"CREATE PROCEDURE $(functionName)(schemaName varchar(50), prefixName varchar(50),                                                                                                                       " + 
"                                tableName varchar(50), channelName varchar(50), dmlType varchar(1), triggerHistId int,                                                                                 " + 
"                                transactionId varchar(1000), externalData varchar(50), pkData varchar(32672), rowData varchar(32672), oldRowData varchar(32672))                                       " + 
"                                PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME                                                                                                     " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.insertData'                                                                                                            " );


        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                            \n" + 
" AFTER INSERT ON $(schemaName)$(tableName)                               \n" + 
" REFERENCING NEW AS NEW                                                  \n" + 
" FOR EACH ROW MODE DB2SQL                                                \n" + 
" call sym_insert_data(                                                   \n" + 
"   '$(defaultSchema)', 'sym', '$(targetTableName)',                      \n" + 
"   '$(channelName)', 'I', $(triggerHistoryId),                           \n" + 
"   $(txIdExpression),                                                    \n" + 
"   $(externalSelect),                                                    \n" + 
"   null,                                                                 \n" + 
"   case when $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)\n" + 
"   then $(columns)                                                       \n" + 
"   else null end,                                                        \n" + 
"   null)                                                                 \n" );
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                            \n" + 
" AFTER UPDATE ON $(schemaName)$(tableName)                               \n" + 
" REFERENCING OLD AS OLD NEW AS NEW                                       \n" + 
" FOR EACH ROW MODE DB2SQL                                                \n" + 
" call sym_insert_data(                                                   \n" + 
"   '$(defaultSchema)', 'sym', '$(targetTableName)',                      \n" + 
"   '$(channelName)', 'U', $(triggerHistoryId),                           \n" + 
"   $(txIdExpression),                                                    \n" + 
"   $(externalSelect),                                                    \n" + 
"   $(oldKeys),                                                           \n" + 
"   case when $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)\n" + 
"   then $(columns)                                                       \n" + 
"   else null end,                                                        \n" + 
"   $(oldColumns))                                                        \n" );
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                            \n" + 
" AFTER DELETE ON $(schemaName)$(tableName)                               \n" + 
" REFERENCING OLD AS OLD                                                  \n" + 
" FOR EACH ROW MODE DB2SQL                                                \n" + 
" call sym_insert_data(                                                   \n" + 
"   '$(defaultSchema)', 'sym', '$(targetTableName)',                      \n" + 
"   '$(channelName)', 'D', $(triggerHistoryId),                           \n" + 
"   $(txIdExpression),                                                    \n" + 
"   $(externalSelect),                                                    \n" + 
"   case when $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)\n" + 
"   then $(oldKeys)                                                       \n" + 
"   else null end, null, $(oldColumns))                                   \n" );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t  where $(whereClause)                                                                                                                               " );
    }


}