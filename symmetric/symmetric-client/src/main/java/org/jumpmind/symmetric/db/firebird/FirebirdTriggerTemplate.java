package org.jumpmind.symmetric.db.firebird;

import java.util.HashMap;

import org.jumpmind.symmetric.db.TriggerTemplate;

public class FirebirdTriggerTemplate extends TriggerTemplate {

    public FirebirdTriggerTemplate() { 
        functionInstalledSql = "select count(*) from rdb$functions where rdb$function_name = upper('$(functionName)')" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || sym_escape(substring($(tableAlias).$(columnName) from 1)) || '\"' end" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || sym_escape(substring($(tableAlias).$(columnName) from 1)) || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || sym_hex($(tableAlias).$(columnName)) || '\"' end" ;
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
"declare external function $(functionName) cstring(32660)                                                                                                                                               " + 
"                                returns cstring(32660) free_it entry_point 'sym_escape' module_name 'sym_udf'                                                                                          " );
        functionTemplatesToInstall.put("hex" ,
"declare external function $(functionName) blob                                                                                                                                                         " + 
"                                returns cstring(32660) free_it entry_point 'sym_hex' module_name 'sym_udf'                                                                                             " );

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) for $(schemaName)$(tableName) after insert as                                                                                                                            " + 
"                                declare variable id bigint;                                                                                                                                            " + 
"                                begin                                                                                                                                                                  " + 
"                                  if ($(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               " + 
"                                  begin                                                                                                                                                                " + 
"                                    select gen_id($(defaultSchema)GEN_$(prefixName)_data_data_id, 1) from rdb$database into :id;                                                                       " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (data_id, table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                               " + 
"                                    values(                                                                                                                                                            " + 
"                                      :id,                                                                                                                                                             " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'I',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      rdb$get_context('USER_SESSION', 'sync_node_disabled'),                                                                                                           " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) for $(schemaName)$(tableName) after update as                                                                                                                            " + 
"                                declare variable id bigint;                                                                                                                                            " + 
"                                begin                                                                                                                                                                  " + 
"                                  if ($(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               " + 
"                                  begin                                                                                                                                                                " + 
"                                    select gen_id($(defaultSchema)GEN_$(prefixName)_data_data_id, 1) from rdb$database into :id;                                                                       " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (data_id, table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)            " + 
"                                    values(                                                                                                                                                            " + 
"                                      :id,                                                                                                                                                             " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'U',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      rdb$get_context('USER_SESSION', 'sync_node_disabled'),                                                                                                           " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger  $(triggerName) for $(schemaName)$(tableName) after delete as                                                                                                                           " + 
"                                declare variable id bigint;                                                                                                                                            " + 
"                                begin                                                                                                                                                                  " + 
"                                  if ($(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               " + 
"                                  begin                                                                                                                                                                " + 
"                                    select gen_id($(defaultSchema)GEN_$(prefixName)_data_data_id, 1) from rdb$database into :id;                                                                       " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (data_id, table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                      " + 
"                                    values(                                                                                                                                                            " + 
"                                      :id,                                                                                                                                                             " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'D',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      rdb$get_context('USER_SESSION', 'sync_node_disabled'),                                                                                                           " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}