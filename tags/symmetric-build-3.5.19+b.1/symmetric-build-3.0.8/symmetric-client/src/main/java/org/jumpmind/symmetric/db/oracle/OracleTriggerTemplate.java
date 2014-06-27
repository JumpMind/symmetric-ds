package org.jumpmind.symmetric.db.oracle;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class OracleTriggerTemplate extends AbstractTriggerTemplate {

    public OracleTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        // @formatter:off         
        functionInstalledSql = "select count(*) from user_source where line = 1 and ((type = 'FUNCTION' and name=upper('$(functionName)')) or (name||'_'||type=upper('$(functionName)')))" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, $(oracleToClob)'', '\"'||replace(replace($(oracleToClob)$(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')||'\"')" ;
        geometryColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then to_clob('') else '\"'||replace(replace(SDO_UTIL.TO_WKTGEOMETRY($(tableAlias).\"$(columnName)\"),'\\','\\\\'),'\"','\\\"')||'\"' end";
        numberColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', '\"'||cast($(tableAlias).\"$(columnName)\" as number(30,10))||'\"')" ;
        datetimeColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.FF3')),'\"'))" ;
        dateTimeWithTimeZoneColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')),'\"'))" ;        
        timeColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS')),'\"'))" ;
        dateColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS')),'\"'))" ;
        clobColumnTemplate = "decode(dbms_lob.getlength($(tableAlias).\"$(columnName)\"), null, to_clob(''), '\"'||replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')||'\"')" ;
        blobColumnTemplate = "decode(dbms_lob.getlength($(tableAlias).\"$(columnName)\"), null, to_clob(''), '\"'||sym_blob2clob($(tableAlias).\"$(columnName)\")||'\"')" ;
        booleanColumnTemplate = "decode($(tableAlias).\"$(columnName)\", null, '', '\"'||cast($(tableAlias).\"$(columnName)\" as number(30,10))||'\"')" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = ":new" ;
        oldTriggerValue = ":old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

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

        functionTemplatesToInstall.put("wkt2geom" ,
                "  CREATE OR REPLACE                                                                                                         " + 
                "    FUNCTION $(functionName)(                            " + 
                "        clob_in IN CLOB)                                 " + 
                "      RETURN SDO_GEOMETRY                                " +
                "    AS                                                   " + 
                "      v_out SDO_GEOMETRY := NULL;                        " + 
                "    BEGIN                                                " + 
                "      IF clob_in IS NOT NULL THEN                        " + 
                "        IF DBMS_LOB.GETLENGTH(clob_in) > 0 THEN          " +
                "          v_out := SDO_GEOMETRY(clob_in);                " + 
                "        END IF;                                          " + 
                "      END IF;                                            " + 
                "      RETURN v_out;                                      " + 
                "    END $(functionName);                                 ");
                        
        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create or replace trigger $(triggerName)                                         \n" +
"    after insert on $(schemaName)$(tableName)                                    \n" + 
"        for each row begin                                                       \n" + 
"            if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then \n" + 
"                insert into $(defaultSchema)$(prefixName)_data                   \n" +
"         (table_name, event_type, trigger_hist_id, row_data, channel_id,         \n" +
"          transaction_id, source_node_id, external_data, create_time)            \n" + 
"          values(                                                                \n" + 
"          '$(targetTableName)',                                                  \n" + 
"          'I',                                                                   \n" + 
"          $(triggerHistoryId),                                                   \n" + 
"          $(oracleToClob)$(columns),                                             \n" + 
"          '$(channelName)',                                                      \n" + 
"          $(txIdExpression),                                                     \n" + 
"          sym_pkg.disable_node_id,                                               \n" + 
"          $(externalSelect),                                                     \n" + 
"          CURRENT_TIMESTAMP                                                      \n" + 
"         );                                                                      \n" + 
"           end if;                                                               \n" + 
"        end;                                                                     \n");
        
        sqlTemplates.put("updateTriggerTemplate" ,
"create or replace trigger $(triggerName) after update on $(schemaName)$(tableName)                                                                                                                     \n" + 
"                                for each row begin                                                                                                                                                     \n" + 
"                                  declare                                                                                                                                                              \n" + 
"                                    var_row_data $(oracleLobType);                                                                                                                                     \n" + 
"                                    var_old_data $(oracleLobType);                                                                                                                                     \n" + 
"                                  begin                                                                                                                                                                \n" + 
"                                  if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 \n" + 
"                                    select $(oracleToClob)$(columns) into var_row_data from dual;                                                                                                      \n" + 
"                                    select $(oracleToClob)$(oldColumns) into var_old_data from dual;                                                                                                   \n" + 
"                                    if $(dataHasChangedCondition) then                                                                                                                                 \n" + 
"                                      insert into $(defaultSchema)$(prefixName)_data                                                                                                                   \n" +
"   (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                                      \n" + 
"                                      values(                                                                                                                                                          \n" + 
"                                        '$(targetTableName)',                                                                                                                                          \n" + 
"                                        'U',                                                                                                                                                           \n" + 
"                                        $(triggerHistoryId),                                                                                                                                           \n" + 
"                                        $(oldKeys),                                                                                                                                                    \n" + 
"                                        var_row_data,                                                                                                                                                  \n" + 
"                                        var_old_data,                                                                                                                                                  \n" + 
"                                        '$(channelName)',                                                                                                                                              \n" + 
"                                        $(txIdExpression),                                                                                                                                             \n" + 
"                                        sym_pkg.disable_node_id,                                                                                                                                       \n" + 
"                                        $(externalSelect),                                                                                                                                             \n" + 
"                                        CURRENT_TIMESTAMP                                                                                                                                              \n" + 
"                                      );                                                                                                                                                               \n" + 
"                                    end if;                                                                                                                                                            \n" + 
"                                  end if;                                                                                                                                                              \n" + 
"                                end;                                                                                                                                                                   \n" + 
"                                end;                                                                                                                                                                   \n" );
        
        sqlTemplates.put("deleteTriggerTemplate" ,
"create or replace trigger  $(triggerName) after delete on $(schemaName)$(tableName)                                                                                                                    \n" + 
"                                for each row begin                                                                                                                                                     \n" + 
"                                  if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                                 \n" + 
"                                    insert into $(defaultSchema)$(prefixName)_data                                                                                                                     \n" +
"   (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                                                \n" + 
"                                    values(                                                                                                                                                            \n" + 
"                                      '$(targetTableName)',                                                                                                                                            \n" + 
"                                      'D',                                                                                                                                                             \n" + 
"                                      $(triggerHistoryId),                                                                                                                                             \n" + 
"                                      $(oldKeys),                                                                                                                                                      \n" + 
"                                      $(oracleToClob)$(oldColumns),                                                                                                                                    \n" + 
"                                      '$(channelName)',                                                                                                                                                \n" + 
"                                      $(txIdExpression),                                                                                                                                               \n" + 
"                                      sym_pkg.disable_node_id,                                                                                                                                         \n" + 
"                                      $(externalSelect),                                                                                                                                               \n" + 
"                                      CURRENT_TIMESTAMP                                                                                                                                                \n" + 
"                                    );                                                                                                                                                                 \n" + 
"                                  end if;                                                                                                                                                              \n" + 
"                                end;                                                                                                                                                                   \n" );
        
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(oracleToClob)$(columns) from $(schemaName)$(tableName) t  where $(whereClause)                                                                                                                " );
    }

}