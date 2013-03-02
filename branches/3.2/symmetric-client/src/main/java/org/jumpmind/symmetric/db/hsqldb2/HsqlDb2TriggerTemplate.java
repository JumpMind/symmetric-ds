package org.jumpmind.symmetric.db.hsqldb2;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class HsqlDb2TriggerTemplate extends AbstractTriggerTemplate {

    public HsqlDb2TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect); 
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"'||replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')||'\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"'||$(tableAlias).\"$(columnName)\"||'\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"'||$(tableAlias).\"$(columnName)\"||'\"' end" ;
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"'||replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')||'\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then ''else '\"'||rawtohex($(tableAlias).\"$(columnName)\")||'\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "newrow" ;
        oldTriggerValue = "oldrow" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) after insert on $(schemaName)$(tableName)                                                                                                                                " + 
"                                referencing new row as newrow                                                                                                                                          " + 
"                                for each row begin atomic                                                                                                                                              " + 
"                                  if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'I',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      '$(channelName)', $(txIdExpression), $(prefixName)_get_session('node_value'),                                                                                    " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                                " + 
"                                referencing new row as newrow                                                                                                                                          " + 
"                                            old row as oldrow                                                                                                                                          " + 
"                                for each row begin atomic                                                                                                                                              " + 
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"	                                    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"	                                    values(                                                                                                                                                           " + 
"	                                      '$(targetTableName)',                                                                                                                                           " + 
"	                                      'U',                                                                                                                                                            " + 
"	                                      $(triggerHistoryId),                                                                                                                                            " + 
"	                                      $(oldKeys),                                                                                                                                                     " + 
"	                                      $(columns),                                                                                                                                                     " + 
"	                                      $(oldColumns),                                                                                                                                                  " + 
"	                                      '$(channelName)', $(txIdExpression), $(prefixName)_get_session('node_value'),                                                                                   " + 
"	                                      $(externalSelect),                                                                                                                                              " + 
"	                                      CURRENT_TIMESTAMP                                                                                                                                               " + 
"	                                    );                                                                                                                                                                " + 
"                                  end if;                                                                                                                                                              " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                                " + 
"                                referencing old row as oldrow                                                                                                                                          " + 
"                                for each row begin atomic                                                                                                                                              " + 
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'D',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)', $(txIdExpression), $(prefixName)_get_session('node_value'),                                                                                    " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}