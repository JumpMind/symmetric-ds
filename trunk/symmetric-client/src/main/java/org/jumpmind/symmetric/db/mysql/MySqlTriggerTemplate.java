package org.jumpmind.symmetric.db.mysql;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class MySqlTriggerTemplate extends AbstractTriggerTemplate {

    public MySqlTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect); 
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',replace(replace($(tableAlias).`$(columnName)`,'\\\\','\\\\\\\\'),'\"','\\\\\"'),'\"'))" ;
        geometryColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',replace(replace(astext($(tableAlias).`$(columnName)`),'\\\\','\\\\\\\\'),'\"','\\\\\"'),'\"'))" ;        
        numberColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',cast($(tableAlias).`$(columnName)` as char),'\"'))" ;
        datetimeColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',cast($(tableAlias).`$(columnName)` as char),'\"'))" ;
        clobColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',replace(replace($(tableAlias).`$(columnName)`,'\\\\','\\\\\\\\'),'\"','\\\\\"'),'\"'))" ;
        blobColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',hex($(tableAlias).`$(columnName)`),'\"'))" ;
        booleanColumnTemplate = "if($(tableAlias).`$(columnName)` is null,'',concat('\"',cast($(tableAlias).`$(columnName)` as unsigned),'\"'))" ;
        triggerConcatCharacter = "," ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) after insert on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row begin                                                                                                                                                     " + 
"                                  if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'I',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      concat($(columns)                                                                                                                                                " + 
"                                       ),                                                                                                                                                              " + 
"                                      '$(channelName)', $(txIdExpression), @sync_node_disabled,                                                                                                        " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row begin                                                                                                                                                     " + 
"                              	  DECLARE var_row_data mediumtext;                                                                                                                                      " + 
"                                  DECLARE var_old_data mediumtext;                                                                                                                                     " + 
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                  	set var_row_data = concat($(columns));                                                                                                                              " + 
"                                  	set var_old_data = concat($(oldColumns));                                                                                                                           " + 
"                                  	if $(dataHasChangedCondition) then                                                                                                                                  " + 
"	                                    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"	                                    values(                                                                                                                                                           " + 
"	                                      '$(targetTableName)',                                                                                                                                           " + 
"	                                      'U',                                                                                                                                                            " + 
"	                                      $(triggerHistoryId),                                                                                                                                            " + 
"	                                      concat($(oldKeys)                                                                                                                                               " + 
"	                                       ),                                                                                                                                                             " + 
"	                                      var_row_data,                                                                                                                                                   " + 
"	                                      var_old_data,                                                                                                                                                   " + 
"	                                      '$(channelName)', $(txIdExpression), @sync_node_disabled,                                                                                                       " + 
"	                                      $(externalSelect),                                                                                                                                              " + 
"	                                      CURRENT_TIMESTAMP                                                                                                                                               " + 
"	                                    );                                                                                                                                                                " + 
"	                                end if;                                                                                                                                                               " + 
"                                  end if;                                                                                                                                                              " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row begin                                                                                                                                                     " + 
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'D',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      concat($(oldKeys)                                                                                                                                                " + 
"                                       ),                                                                                                                                                              " + 
"                                       concat($(oldColumns)                                                                                                                                            " + 
"                                       ),                                                                                                                                                              " + 
"                                      '$(channelName)', $(txIdExpression), @sync_node_disabled,                                                                                                        " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select concat($(columns)) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                        " );
    }

}