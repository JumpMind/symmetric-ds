package org.jumpmind.symmetric.db.interbase;

import java.util.HashMap;

import org.jumpmind.symmetric.db.TriggerText;

public class InterbaseTriggerText extends TriggerText {

    public InterbaseTriggerText() { 
        functionInstalledSql = "select count(*) from rdb$functions where rdb$function_name = upper('$(functionName)')" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || sym_escape($(tableAlias).$(columnName)) || '\"' end" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || $(tableAlias).$(columnName) || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || sym_escape($(tableAlias).$(columnName)) || '\"' end" ;
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
"declare external function $(functionName) cstring(4096)                                                                                                                                                " + 
"                                returns cstring(4096) free_it entry_point 'sym_escape' module_name 'sym_udf'                                                                                           " );
        functionTemplatesToInstall.put("hex" ,
"declare external function $(functionName) blob                                                                                                                                                         " + 
"                                returns cstring(4096) free_it entry_point 'sym_hex' module_name 'sym_udf'                                                                                              " );
        functionTemplatesToInstall.put("rtrim" ,
"declare external function $(functionName) cstring(32767)                                                                                                                                               " + 
"                                returns cstring(32767) free_it entry_point 'IB_UDF_rtrim' module_name 'ib_udf'                                                                                         " );

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) for $(schemaName)$(tableName) after insert as                                                                                                                            " + 
"                                declare variable id integer;                                                                                                                                           " + 
"                                declare variable sync_triggers_disabled varchar(30);                                                                                                                   " + 
"                                declare variable sync_node_disabled varchar(30);                                                                                                                       " + 
"                                begin                                                                                                                                                                  " + 
"                                  select context_value from $(prefixName)_context where id = 'sync_triggers_disabled' into :sync_triggers_disabled;                                                    " + 
"                                  if ($(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               " + 
"                                  begin                                                                                                                                                                " + 
"                                    select context_value from $(prefixName)_context where id = 'sync_node_disabled' into :sync_node_disabled;                                                          " + 
"                                    select gen_id($(defaultSchema)gen_$(prefixName)_data_data_id, 1) from rdb$database into :id;                                                                       " + 
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
"                                      :sync_node_disabled,                                                                                                                                             " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) for $(schemaName)$(tableName) after update as                                                                                                                            " + 
"                                declare variable id integer;                                                                                                                                           " + 
"                                declare variable sync_triggers_disabled varchar(30);                                                                                                                   " + 
"                                declare variable sync_node_disabled varchar(30);                                                                                                                       " + 
"                                begin                                                                                                                                                                  " + 
"                                  select context_value from $(prefixName)_context where id = 'sync_triggers_disabled' into :sync_triggers_disabled;                                                    " + 
"                                  if ($(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               " + 
"                                  begin                                                                                                                                                                " + 
"                                    select context_value from $(prefixName)_context where id = 'sync_node_disabled' into :sync_node_disabled;                                                          " + 
"                                    select gen_id($(defaultSchema)gen_$(prefixName)_data_data_id, 1) from rdb$database into :id;                                                                       " + 
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
"                                      :sync_node_disabled,                                                                                                                                             " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger  $(triggerName) for $(schemaName)$(tableName) after delete as                                                                                                                           " + 
"                                declare variable id integer;                                                                                                                                           " + 
"                                declare variable sync_triggers_disabled varchar(30);                                                                                                                   " + 
"                                declare variable sync_node_disabled varchar(30);                                                                                                                       " + 
"                                begin                                                                                                                                                                  " + 
"                                  select context_value from $(prefixName)_context where id = 'sync_triggers_disabled' into :sync_triggers_disabled;                                                    " + 
"                                  if ($(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)) then                                                                                               " + 
"                                  begin                                                                                                                                                                " + 
"                                    select context_value from $(prefixName)_context where id = 'sync_node_disabled' into :sync_node_disabled;                                                          " + 
"                                    select gen_id($(defaultSchema)gen_$(prefixName)_data_data_id, 1) from rdb$database into :id;                                                                       " + 
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
"                                      :sync_node_disabled,                                                                                                                                             " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select sym_rtrim($(columns))||'' from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                 " );
    }

}