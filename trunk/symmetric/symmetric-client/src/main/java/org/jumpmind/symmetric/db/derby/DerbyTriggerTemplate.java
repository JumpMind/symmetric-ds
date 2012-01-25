package org.jumpmind.symmetric.db.derby;

import java.util.HashMap;

import org.jumpmind.symmetric.db.TriggerTemplate;

public class DerbyTriggerTemplate extends TriggerTemplate {

    public DerbyTriggerTemplate() { 
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
"CREATE TRIGGER $(triggerName)                                                                                                                                                                          " + 
"                                AFTER INSERT ON $(schemaName)$(tableName)                                                                                                                              " + 
"                                REFERENCING NEW AS NEW                                                                                                                                                 " + 
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " + 
"                                call sym_insert_data(                                                                                                                                                  " + 
"                                  '$(defaultSchema)', 'sym', '$(targetTableName)',                                                                                                                     " + 
"                                  '$(channelName)', 'I', $(triggerHistoryId),                                                                                                                          " + 
"                                  $(txIdExpression),                                                                                                                                                   " + 
"                                  $(externalSelect),                                                                                                                                                   " + 
"                                  null,                                                                                                                                                                " + 
"                                  case when $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)                                                                                               " + 
"                                  then $(columns)                                                                                                                                                      " + 
"                                  else null end,                                                                                                                                                       " + 
"                                  null)                                                                                                                                                                " );
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                                                                                                                                                          " + 
"                                AFTER UPDATE ON $(schemaName)$(tableName)                                                                                                                              " + 
"                                REFERENCING OLD AS OLD NEW AS NEW                                                                                                                                      " + 
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " + 
"                                call sym_insert_data(                                                                                                                                                  " + 
"                                  '$(defaultSchema)', 'sym', '$(targetTableName)',                                                                                                                     " + 
"                                  '$(channelName)', 'U', $(triggerHistoryId),                                                                                                                          " + 
"                                  $(txIdExpression),                                                                                                                                                   " + 
"                                  $(externalSelect),                                                                                                                                                   " + 
"                                  $(oldKeys),                                                                                                                                                          " + 
"                                  case when $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)                                                                                               " + 
"                                  then $(columns)                                                                                                                                                      " + 
"                                  else null end,                                                                                                                                                       " + 
"                                  $(oldColumns))                                                                                                                                                       " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                                                                                                                                                          " + 
"                                AFTER DELETE ON $(schemaName)$(tableName)                                                                                                                              " + 
"                                REFERENCING OLD AS OLD                                                                                                                                                 " + 
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " + 
"                                call sym_insert_data(                                                                                                                                                  " + 
"                                  '$(defaultSchema)', 'sym', '$(targetTableName)',                                                                                                                     " + 
"                                  '$(channelName)', 'D', $(triggerHistoryId),                                                                                                                          " + 
"                                  $(txIdExpression),                                                                                                                                                   " + 
"                                  $(externalSelect),                                                                                                                                                   " + 
"                                  case when $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)                                                                                               " + 
"                                  then $(oldKeys)                                                                                                                                                      " + 
"                                  else null end, null, $(oldColumns))                                                                                                                                  " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t  where $(whereClause)                                                                                                                               " );
    }

}