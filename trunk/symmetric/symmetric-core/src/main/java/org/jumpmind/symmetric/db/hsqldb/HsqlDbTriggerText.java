package org.jumpmind.symmetric.db.hsqldb;

import org.jumpmind.symmetric.db.TriggerText;
import java.util.HashMap;

public class HsqlDbTriggerText extends TriggerText {

    public HsqlDbTriggerText() { 
        functionInstalledSql = "select count(*) from INFORMATION_SCHEMA.SYSTEM_ALIASES where ALIAS='$(functionName)'" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',cast($(tableAlias)\"$(columnName)\" as varchar(50))),''\"'') end" ;
        datetimeColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        blobColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' when $(tableAlias)\"$(columnName)\" then ''\"1\"'' else ''\"0\"'' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "" ;
        oldTriggerValue = "" ;
        oldColumnPrefix = "OLD_" ;
        newColumnPrefix = "NEW_" ;
        otherColumnTemplate = null;

        functionTemplatesToInstall = new HashMap<String,String>();
        functionTemplatesToInstall.put("base_64_encode" ,
"CREATE ALIAS $(functionName) for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.encodeBase64\";                                                                                                    " );
        functionTemplatesToInstall.put("set_session" ,
"CREATE ALIAS $(functionName) for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.setSession\";                                                                                                      " );
        functionTemplatesToInstall.put("get_session" ,
"CREATE ALIAS $(functionName) for \"org.jumpmind.symmetric.db.hsqldb.HsqlDbFunctions.getSession\";                                                                                                      " );

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL LONGVARCHAR, INSERT_DATA_SQL LONGVARCHAR);                                                                                                           " + 
"                                INSERT INTO $(triggerName)_CONFIG values(                                                                                                                              " + 
"                                'select count(*) from $(virtualOldNewTable) where $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)',                                                       " + 
"                                'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                  (select ''$(targetTableName)'',''I'',$(triggerHistoryId),$(columns), ''$(channelName)'', $(txIdExpression), sym_get_session(''node_value''), $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))' " + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(triggerName) AFTER INSERT ON $(tableName) FOR EACH ROW QUEUE 0 CALL \"org.jumpmind.symmetric.db.hsqldb.HsqlDbTrigger\";                               " );
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL LONGVARCHAR, INSERT_DATA_SQL LONGVARCHAR);                                                                                                           " + 
"                                INSERT INTO $(triggerName)_CONFIG values(                                                                                                                              " + 
"                                  'select count(*) from $(virtualOldNewTable) where $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)',                                                     " + 
"                                  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    (select ''$(targetTableName)'',''U'',$(triggerHistoryId),$(oldKeys),$(columns),$(oldColumns), ''$(channelName)'', $(txIdExpression), sym_get_session(''node_value''), $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'" + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(triggerName) AFTER UPDATE ON $(tableName) FOR EACH ROW QUEUE 0 CALL \"org.jumpmind.symmetric.db.hsqldb.HsqlDbTrigger\";                               " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL LONGVARCHAR, INSERT_DATA_SQL LONGVARCHAR);                                                                                                           " + 
"                                INSERT INTO $(triggerName)_CONFIG values(                                                                                                                              " + 
"                                  'select count(*) from $(virtualOldNewTable) where $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)',                                                     " + 
"                                  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    (select ''$(targetTableName)'',''D'',$(triggerHistoryId),$(oldKeys),$(oldColumns),''$(channelName)'', $(txIdExpression), sym_get_session(''node_value''), $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'" + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(triggerName) AFTER DELETE ON $(tableName) FOR EACH ROW QUEUE 0 CALL \"org.jumpmind.symmetric.db.hsqldb.HsqlDbTrigger\";                               " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}