package org.jumpmind.symmetric.db.postgresql;

import java.util.HashMap;

import org.jumpmind.symmetric.db.TriggerText;

public class PostgreSqlTriggerText extends TriggerText {

    public PostgreSqlTriggerText() { 
        functionInstalledSql = "select count(*) from information_schema.routines " + 
"                        where routine_name = '$(functionName)' and specific_schema = '$(defaultSchema)'" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        xmlColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        arrayColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast($(tableAlias).\"$(columnName)\" as varchar) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US') || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || encode($(tableAlias).\"$(columnName)\", 'base64') || '\"' end" ;
        wrappedBlobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || $(defaultSchema)$(prefixName)_largeobject($(tableAlias).\"$(columnName)\") || '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast($(tableAlias).\"$(columnName)\" as varchar) || '\"' end" ;

        functionTemplatesToInstall = new HashMap<String,String>();
        functionTemplatesToInstall.put("triggers_disabled" ,
"CREATE or REPLACE FUNCTION $(defaultSchema)$(functionName)() RETURNS INTEGER AS $$                                                                                                                     " + 
"                                DECLARE                                                                                                                                                                " + 
"                                  triggerDisabled INTEGER;                                                                                                                                             " + 
"                                BEGIN                                                                                                                                                                  " + 
"                                  select current_setting('symmetric.triggers_disabled') into triggerDisabled;                                                                                          " + 
"                                  return triggerDisabled;                                                                                                                                              " + 
"                                EXCEPTION WHEN OTHERS THEN                                                                                                                                             " + 
"                                  return 0;                                                                                                                                                            " + 
"                                END;                                                                                                                                                                   " + 
"                                $$ LANGUAGE plpgsql;                                                                                                                                                   " );
        functionTemplatesToInstall.put("node_disabled" ,
"CREATE or REPLACE FUNCTION $(defaultSchema)$(functionName)() RETURNS VARCHAR AS $$                                                                                                                     " + 
"                                DECLARE                                                                                                                                                                " + 
"                                  nodeId VARCHAR(50);                                                                                                                                                  " + 
"                                BEGIN                                                                                                                                                                  " + 
"                                  select current_setting('symmetric.node_disabled') into nodeId;                                                                                                       " + 
"                                  return nodeId;                                                                                                                                                       " + 
"                                EXCEPTION WHEN OTHERS THEN                                                                                                                                             " + 
"                                  return '';                                                                                                                                                           " + 
"                                END;                                                                                                                                                                   " + 
"                                $$ LANGUAGE plpgsql;                                                                                                                                                   " );
        functionTemplatesToInstall.put("largeobject" ,
"CREATE OR REPLACE FUNCTION $(defaultSchema)$(functionName)(objectId oid) RETURNS text AS $$                                                                                                            " + 
"                                DECLARE                                                                                                                                                                " + 
"                                  encodedBlob text;                                                                                                                                                    " + 
"                                  encodedBlobPage text;                                                                                                                                                " + 
"                                BEGIN                                                                                                                                                                  " + 
"                                  encodedBlob := '';                                                                                                                                                   " + 
"                                  FOR encodedBlobPage IN SELECT encode(data, 'escape')                                                                                                                 " + 
"                                  FROM pg_largeobject WHERE loid = objectId ORDER BY pageno LOOP                                                                                                       " + 
"                                    encodedBlob := encodedBlob || encodedBlobPage;                                                                                                                     " + 
"                                  END LOOP;                                                                                                                                                            " + 
"                                  RETURN encode(decode(encodedBlob, 'escape'), 'base64');                                                                                                              " + 
"                                EXCEPTION WHEN OTHERS THEN                                                                                                                                             " + 
"                                  RETURN '';                                                                                                                                                           " + 
"                                END                                                                                                                                                                    " + 
"                                $$ LANGUAGE plpgsql;                                                                                                                                                   " );

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                " + 
"                                begin                                                                                                                                                                  " + 
"                                  if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                        " + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'I',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                  return null;                                                                                                                                                         " + 
"                                end;                                                                                                                                                                   " + 
"                                $function$ language plpgsql;                                                                                                                                           " );
        sqlTemplates.put("insertPostTriggerTemplate" ,
"create trigger $(triggerName) after insert on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                " + 
"                                begin                                                                                                                                                                  " + 
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                     " + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'U',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(columns),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                  return null;                                                                                                                                                         " + 
"                                end;                                                                                                                                                                   " + 
"                                $function$ language plpgsql;                                                                                                                                           " );
        sqlTemplates.put("updatePostTriggerTemplate" ,
"create trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                                                                " + 
"                                begin                                                                                                                                                                  " + 
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     " + 
"                                    (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                               " + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'D',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(oldColumns),                                                                                                                                                   " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      $(defaultSchema)$(prefixName)_node_disabled(),                                                                                                                   " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                  return null;                                                                                                                                                         " + 
"                                end;                                                                                                                                                                   " + 
"                                $function$ language plpgsql;                                                                                                                                           " );
        sqlTemplates.put("deletePostTriggerTemplate" ,
"create trigger $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                                " + 
"                                for each row execute procedure $(schemaName)f$(triggerName)();                                                                                                         " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }
    
    @Override
    protected boolean requiresWrappedBlobTemplateForBlobType() {
        return true;
    }

}