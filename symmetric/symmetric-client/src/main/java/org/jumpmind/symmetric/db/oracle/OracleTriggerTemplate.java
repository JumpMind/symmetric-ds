package org.jumpmind.symmetric.db.oracle;

import java.util.HashMap;

import org.jumpmind.symmetric.db.TriggerTemplate;

public class OracleTriggerTemplate extends TriggerTemplate {

    public OracleTriggerTemplate() { 
        functionInstalledSql = "select count(*) from user_source where line = 1 and ((type = 'FUNCTION' and name=upper('$(functionName)')) or (name||'_'||type=upper('$(functionName)')))" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, $(oracleToClob)'', '\"'||replace(replace($(oracleToClob)$(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')||'\"')" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', '\"'||cast($(tableAlias).\"$(columnName)\" as number(30,10))||'\"')" ;
        datetimeColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.FF3')),'\"'))" ;
        timeColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS')),'\"'))" ;
        dateColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS')),'\"'))" ;
        clobColumnTemplate = "decode(dbms_lob.getlength($(tableAlias).\"$(columnName)\"), null, to_clob(''), '\"'||replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')||'\"')" ;
        blobColumnTemplate = "decode(dbms_lob.getlength($(tableAlias).\"$(columnName)\"), null, to_clob(''), '\"'||sym_blob2clob($(tableAlias).\"$(columnName)\")||'\"')" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', '\"'||cast($(tableAlias).\"$(columnName)\" as number(30,10))||'\"')" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = ":new" ;
        oldTriggerValue = ":old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = null;

        functionTemplatesToInstall = new HashMap<String,String>();
        functionTemplatesToInstall.put("blob2clob" ,
"CREATE OR REPLACE FUNCTION $(functionName) (blob_in IN BLOB)                                                                                                                                           " + 
"                                  RETURN CLOB                                                                                                                                                          " + 
"                                AS                                                                                                                                                                     " + 
"                                    v_clob    CLOB := null;                                                                                                                                            " + 
"                                    v_varchar VARCHAR2(32767);                                                                                                                                         " + 
"                                    v_start   PLS_INTEGER := 1;                                                                                                                                        " + 
"                                    v_buffer  PLS_INTEGER := 999;                                                                                                                                      " + 
"                                BEGIN                                                                                                                                                                  " + 
"                                    IF blob_in IS NOT NULL THEN                                                                                                                                        " + 
"                                        IF DBMS_LOB.GETLENGTH(blob_in) > 0 THEN                                                                                                                        " + 
"                                            DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);                                                                                                                    " + 
"                                            FOR i IN 1..CEIL(DBMS_LOB.GETLENGTH(blob_in) / v_buffer)                                                                                                   " + 
"                                            LOOP                                                                                                                                                       " + 
"                                                v_varchar := UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.base64_encode(DBMS_LOB.SUBSTR(blob_in, v_buffer, v_start)));                                          " + 
"                                                v_varchar := REPLACE(v_varchar,CHR(13)||CHR(10));                                                                                                      " + 
"                                                DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_varchar), v_varchar);                                                                                            " + 
"                                                v_start := v_start + v_buffer;                                                                                                                         " + 
"                                            END LOOP;                                                                                                                                                  " + 
"                                        END IF;                                                                                                                                                        " + 
"                                    END IF;                                                                                                                                                            " + 
"                                    RETURN v_clob;                                                                                                                                                     " + 
"                                END $(functionName);                                                                                                                                                   " );
        functionTemplatesToInstall.put("transaction_id" ,
"CREATE OR REPLACE function $(functionName)                                                                                                                                                             " + 
"                                    return varchar is                                                                                                                                                  " + 
"                                    begin                                                                                                                                                              " + 
"                                       return DBMS_TRANSACTION.local_transaction_id(false);                                                                                                            " + 
"                                    end;                                                                                                                                                               " );
        functionTemplatesToInstall.put("trigger_disabled" ,
"CREATE OR REPLACE function $(functionName) return varchar is                                                                                                                                           " + 
"                                  begin                                                                                                                                                                " + 
"                                     return sym_pkg.disable_trigger;                                                                                                                                   " + 
"                                  end;                                                                                                                                                                 " );
        functionTemplatesToInstall.put("pkg_package" ,
"CREATE OR REPLACE package sym_pkg as                                                                                                                                                                   " + 
"                                    disable_trigger pls_integer;                                                                                                                                       " + 
"                                    disable_node_id varchar(50);                                                                                                                                       " + 
"                                    procedure setValue (a IN number);                                                                                                                                  " + 
"                                    procedure setNodeValue (node_id IN varchar);                                                                                                                       " + 
"                                end sym_pkg;                                                                                                                                                           " );
        functionTemplatesToInstall.put("pkg_package body" ,
"CREATE OR REPLACE package body sym_pkg as                                                                                                                                                              " + 
"                                    procedure setValue(a IN number) is                                                                                                                                 " + 
"                                    begin                                                                                                                                                              " + 
"                                         sym_pkg.disable_trigger:=a;                                                                                                                                   " + 
"                                    end;                                                                                                                                                               " + 
"                                    procedure setNodeValue(node_id IN varchar) is                                                                                                                      " + 
"                                    begin                                                                                                                                                              " + 
"                                         sym_pkg.disable_node_id := node_id;                                                                                                                           " + 
"                                    end;                                                                                                                                                               " + 
"                                end sym_pkg;                                                                                                                                                           " );

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create or replace trigger $(triggerName) after insert on $(schemaName)$(tableName)                                                                                                                     " + 
"                                for each row begin                                                                                                                                                     " + 
"                                  if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'I',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oracleToClob)$(columns),                                                                                                                                       " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      sym_pkg.disable_node_id,                                                                                                                                         " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                end;                                                                                                                                                                   " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create or replace trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                     " + 
"                                for each row begin                                                                                                                                                     " + 
"                                  declare                                                                                                                                                              " + 
"                                    var_row_data $(oracleLobType);                                                                                                                                     " + 
"                                    var_old_data $(oracleLobType);                                                                                                                                     " + 
"                                  begin                                                                                                                                                                " + 
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    select $(oracleToClob)$(columns) into var_row_data from dual;                                                                                                      " + 
"                                    select $(oracleToClob)$(oldColumns) into var_old_data from dual;                                                                                                   " + 
"                                    if $(dataHasChangedCondition) then                                                                                                                                 " + 
"                                      insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                      values(                                                                                                                                                          " + 
"                                        '$(targetTableName)',                                                                                                                                          " + 
"                                        'U',                                                                                                                                                           " + 
"                                        $(triggerHistoryId),                                                                                                                                           " + 
"                                        $(oldKeys),                                                                                                                                                    " + 
"                                        var_row_data,                                                                                                                                                  " + 
"                                        var_old_data,                                                                                                                                                  " + 
"                                        '$(channelName)',                                                                                                                                              " + 
"                                        $(txIdExpression),                                                                                                                                             " + 
"                                        sym_pkg.disable_node_id,                                                                                                                                       " + 
"                                        $(externalSelect),                                                                                                                                             " + 
"                                        CURRENT_TIMESTAMP                                                                                                                                              " + 
"                                      );                                                                                                                                                               " + 
"                                    end if;                                                                                                                                                            " + 
"                                  end if;                                                                                                                                                              " + 
"                                end;                                                                                                                                                                   " + 
"                                end;                                                                                                                                                                   " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create or replace trigger  $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                    " + 
"                                for each row begin                                                                                                                                                     " + 
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 " + 
"                                    insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    values(                                                                                                                                                            " + 
"                                      '$(targetTableName)',                                                                                                                                            " + 
"                                      'D',                                                                                                                                                             " + 
"                                      $(triggerHistoryId),                                                                                                                                             " + 
"                                      $(oldKeys),                                                                                                                                                      " + 
"                                      $(oracleToClob)$(oldColumns),                                                                                                                                    " + 
"                                      '$(channelName)',                                                                                                                                                " + 
"                                      $(txIdExpression),                                                                                                                                               " + 
"                                      sym_pkg.disable_node_id,                                                                                                                                         " + 
"                                      $(externalSelect),                                                                                                                                               " + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                " + 
"                                    );                                                                                                                                                                 " + 
"                                  end if;                                                                                                                                                              " + 
"                                end;                                                                                                                                                                   " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(oracleToClob)$(columns) from $(schemaName)$(tableName) t  where $(whereClause)                                                                                                                " );
    }

}