package org.jumpmind.symmetric.db.informix;

import org.jumpmind.symmetric.db.TriggerText;
import java.util.HashMap;

public class InformixTriggerText extends TriggerText {

    public InformixTriggerText() { 
        functionInstalledSql = "select count(*) from sysprocedures where procname = '$(functionName)' and owner = (select trim(user) from sysmaster:sysdual)" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "rtrim(case when $(tableAlias).$(columnName) is null then '' else '\"' || replace(replace($(tableAlias).$(columnName), '\\', '\\\\'), '\"', '\\\"') || '\"' end)" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "''" ;
        blobColumnTemplate = "''" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' when $(tableAlias).$(columnName) then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = null;

        functionTemplatesToInstall = new HashMap<String,String>();
        functionTemplatesToInstall.put("triggers_disabled" ,
"create function $(defaultSchema)$(functionName)() returning boolean;                                                                                                                                   " + 
"                                   define global symmetric_triggers_disabled boolean default 'f';                                                                                                      " + 
"                                   return symmetric_triggers_disabled;                                                                                                                                 " + 
"                                end function;                                                                                                                                                          " );
        functionTemplatesToInstall.put("triggers_set_disabled" ,
"create function $(defaultSchema)$(functionName)(is_disabled boolean) returning boolean;                                                                                                                " + 
"                                   define global symmetric_triggers_disabled boolean default 'f';                                                                                                      " + 
"                                   let symmetric_triggers_disabled = is_disabled;                                                                                                                      " + 
"                                   return symmetric_triggers_disabled;                                                                                                                                 " + 
"                                end function;                                                                                                                                                          " );
        functionTemplatesToInstall.put("node_disabled" ,
"create function $(defaultSchema)$(functionName)() returning varchar(50);                                                                                                                               " + 
"                                   define global symmetric_node_disabled varchar(50) default null;                                                                                                     " + 
"                                   return symmetric_node_disabled;                                                                                                                                     " + 
"                                end function;                                                                                                                                                          " );
        functionTemplatesToInstall.put("node_set_disabled" ,
"create function $(defaultSchema)$(functionName)(node_id varchar(50)) returning integer;                                                                                                                " + 
"                                   define global symmetric_node_disabled varchar(50) default null;                                                                                                     " + 
"                                   let symmetric_node_disabled = node_id;                                                                                                                              " + 
"                                   return 1;                                                                                                                                                           " + 
"                                end function;                                                                                                                                                          " );

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) insert on $(schemaName)$(tableName)                                                                                                                                      " + 
"                                referencing new as new                                                                                                                                                 " + 
"                                for each row when ($(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)) (                                                                                     " + 
"                                insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                values(                                                                                                                                                                " + 
"                                  '$(targetTableName)',                                                                                                                                                " + 
"                                  'I',                                                                                                                                                                 " + 
"                                  $(triggerHistoryId),                                                                                                                                                 " + 
"                                  $(columns),                                                                                                                                                          " + 
"                                  '$(channelName)',                                                                                                                                                    " + 
"                                  $(txIdExpression),                                                                                                                                                   " + 
"                                  $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                       " + 
"                                  $(externalSelect),                                                                                                                                                   " + 
"                                  CURRENT                                                                                                                                                              " + 
"                                ));                                                                                                                                                                    " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) update on $(schemaName)$(tableName)                                                                                                                                      " + 
"                                referencing old as old new as new                                                                                                                                      " + 
"                                for each row when ($(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)) (                                                                                     " + 
"                                insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                values(                                                                                                                                                                " + 
"                                  '$(targetTableName)',                                                                                                                                                " + 
"                                  'U',                                                                                                                                                                 " + 
"                                  $(triggerHistoryId),                                                                                                                                                 " + 
"                                  $(oldKeys),                                                                                                                                                          " + 
"                                  $(columns),                                                                                                                                                          " + 
"                                  $(oldColumns),                                                                                                                                                       " + 
"                                  '$(channelName)',                                                                                                                                                    " + 
"                                  $(txIdExpression),                                                                                                                                                   " + 
"                                  $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                       " + 
"                                  $(externalSelect),                                                                                                                                                   " + 
"                                  CURRENT                                                                                                                                                              " + 
"                                ));                                                                                                                                                                    " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) delete on $(schemaName)$(tableName)                                                                                                                                      " + 
"                                referencing old as old                                                                                                                                                 " + 
"                                for each row when ($(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)) (                                                                                     " + 
"                                insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                values(                                                                                                                                                                " + 
"                                  '$(targetTableName)',                                                                                                                                                " + 
"                                  'D',                                                                                                                                                                 " + 
"                                  $(triggerHistoryId),                                                                                                                                                 " + 
"                                  $(oldKeys),                                                                                                                                                          " + 
"                                  $(oldColumns),                                                                                                                                                       " + 
"                                  '$(channelName)',                                                                                                                                                    " + 
"                                  $(txIdExpression),                                                                                                                                                   " + 
"                                  $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                       " + 
"                                  $(externalSelect),                                                                                                                                                   " + 
"                                  CURRENT                                                                                                                                                              " + 
"                                ));                                                                                                                                                                    " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t  where $(whereClause)                                                                                                                               " );
    }

}