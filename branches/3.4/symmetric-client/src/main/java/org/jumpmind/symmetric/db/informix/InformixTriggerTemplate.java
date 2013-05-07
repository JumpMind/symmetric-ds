package org.jumpmind.symmetric.db.informix;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class InformixTriggerTemplate extends AbstractTriggerTemplate {

    public InformixTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect); 
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "rtrim(case when $(tableAlias).$(columnName) is null then '' else '\"' || replace(replace($(tableAlias).$(columnName), '\\', '\\\\'), '\"', '\\\"') || '\"' end)" ;
        numberColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        clobColumnTemplate = "''" ;
        blobColumnTemplate = "''" ;
        booleanColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' when $(tableAlias).$(columnName) then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

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