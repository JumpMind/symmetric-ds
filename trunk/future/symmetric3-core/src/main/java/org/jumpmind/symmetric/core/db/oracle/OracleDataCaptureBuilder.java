package org.jumpmind.symmetric.core.db.oracle;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.AbstractDataCaptureBuilder;
import org.jumpmind.symmetric.core.model.Parameters;

public class OracleDataCaptureBuilder extends AbstractDataCaptureBuilder {

    public OracleDataCaptureBuilder(IDbDialect dbDialect) {
        super(dbDialect);
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }
    
    @Override
    protected String getClobColumnTemplate() {
        return "decode(dbms_lob.getlength($(tableAlias).\"$(columnName)\"), null, to_clob(''), '\"'||replace(replace($(tableAlias).\"$(columnName)\",'\','\\'),'\"','\\\"')||'\"')";
    }

    @Override
    protected String getNewTriggerValue() {
        return ":new";
    }

    @Override
    protected String getOldTriggerValue() {
        return ":old";
    }

    @Override
    protected String getBlobColumnTemplate() {
        return "decode(dbms_lob.getlength($(tableAlias).\"$(columnName)\"), null, to_clob(''), '\"'||$[sym.sync.table.prefix]_blob2clob($(tableAlias).\"$(columnName)\")||'\"')";
    }

    @Override
    protected String getWrappedBlobColumnTemplate() {
        return null;
    }

    @Override
    protected Map<String, String> getFunctionTemplatesToInstall() {
        Map<String, String> functionTemplatesToInstall = new HashMap<String, String>();
        functionTemplatesToInstall
                .put("blob2clob",
                        "CREATE OR REPLACE FUNCTION $(functionName) (blob_in IN BLOB)                                                                                 "
                                + "          RETURN CLOB                                                                                                                        "
                                + "        AS                                                                                                                                   "
                                + "            v_clob    CLOB := null;                                                                                                          "
                                + "            v_varchar VARCHAR2(32767);                                                                                                       "
                                + "            v_start   PLS_INTEGER := 1;                                                                                                      "
                                + "            v_buffer  PLS_INTEGER := 999;                                                                                                    "
                                + "        BEGIN                                                                                                                                "
                                + "            IF blob_in IS NOT NULL THEN                                                                                                      "
                                + "                IF DBMS_LOB.GETLENGTH(blob_in) > 0 THEN                                                                                      "
                                + "                    DBMS_LOB.CREATETEMPORARY(v_clob, TRUE);                                                                                  "
                                + "                    FOR i IN 1..CEIL(DBMS_LOB.GETLENGTH(blob_in) / v_buffer)                                                                 "
                                + "                    LOOP                                                                                                                     "
                                + "                        v_varchar := UTL_RAW.CAST_TO_VARCHAR2(UTL_ENCODE.base64_encode(DBMS_LOB.SUBSTR(blob_in, v_buffer, v_start)));        "
                                + "                        v_varchar := REPLACE(v_varchar,CHR(13)||CHR(10));                                                                    "
                                + "                        DBMS_LOB.WRITEAPPEND(v_clob, LENGTH(v_varchar), v_varchar);                                                          "
                                + "                        v_start := v_start + v_buffer;                                                                                       "
                                + "                    END LOOP;                                                                                                                "
                                + "                END IF;                                                                                                                      "
                                + "            END IF;                                                                                                                          "
                                + "            RETURN v_clob;                                                                                                                   "
                                + "        END $(functionName);");
        functionTemplatesToInstall.put("transaction_id",
                "CREATE OR REPLACE function $(functionName)             "
                        + "   return varchar is             " + "   begin             "
                        + "      return DBMS_TRANSACTION.local_transaction_id(false);             "
                        + "   end;             ");
        functionTemplatesToInstall.put("trigger_disabled",
                "CREATE OR REPLACE function $(functionName) return varchar is           "
                        + "    begin           "
                        + "       return $[sym.sync.table.prefix]_pkg.disable_trigger;          "
                        + "    end;          ");
        functionTemplatesToInstall.put("set_node_value",
                "CREATE OR REPLACE package $[sym.sync.table.prefix]_pkg as          "
                        + "      disable_trigger pls_integer;          "
                        + "      disable_node_id varchar(50);          "
                        + "      procedure setValue (a IN number);          "
                        + "      procedure setNodeValue (node_id IN varchar);          "
                        + "  end $[sym.sync.table.prefix]_pkg;          ");
        functionTemplatesToInstall
                .put("disable_trigger",
                        "CREATE OR REPLACE package body $[sym.sync.table.prefix]_pkg as          "
                                + "       procedure setValue(a IN number) is          "
                                + "       begin          "
                                + "            $[sym.sync.table.prefix]_pkg.disable_trigger:=a;          "
                                + "       end;          "
                                + "       procedure setNodeValue(node_id IN varchar) is          "
                                + "       begin          "
                                + "            $[sym.sync.table.prefix]_pkg.disable_node_id := node_id;          "
                                + "       end;          "
                                + "   end $[sym.sync.table.prefix]_pkg;          ");

        return functionTemplatesToInstall;
    }

    @Override
    protected String getFunctionInstalledSqlTemplate() {
        return "select count(*) from user_source where line = 1 and (type = 'FUNCTION' or type = 'PACKAGE') and name=upper('$(functionName)'";
    }

    @Override
    protected String getStringColumnTemplate() {
        return "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"')),'\"'))";
    }

    @Override
    protected String getNumberColumnTemplate() {
        return String.format("decode($(tableAlias).\"$(columnName)\", null, '', '\"'||cast($(tableAlias).\"$(columnName)\" as number(%s))||'\"')", dbDialect.getParameters().get(Parameters.TRIGGER_NUMBER_PRECISION, "30,10"));
    }
    
    @Override
    protected String getDateTimeWithTimeZoneTemplate() {
        return "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.FF TZH:TZM')),'\"'))";
    }

    @Override
    protected String getDateTimeColumnTemplate() {
        return "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.FF3')),'\"'))";
    }

    @Override
    protected String getBooleanColumnTemplate() {
        return getNumberColumnTemplate();
    }

    @Override
    protected String getTimeColumnTemplate() {
        return "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS')),'\"'))";
    }

    @Override
    protected String getDateColumnTemplate() {
        return "decode($(tableAlias).\"$(columnName)\", null, '', concat(concat('\"',to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS')),'\"'))";
    }

    @Override
    protected String getTriggerConcatCharacter() {
        return "||";
    }

    @Override
    protected String getInsertTriggerTemplate() {
        return "   create or replace trigger $(triggerName) after insert on $(schemaName)$(tableName)  "
                + "  for each row begin   "
                + "    if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then   "
                + "      insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)   "
                + "      values(   "
                + "        '$(targetTableName)',   "
                + "        'I',   "
                + "        $(triggerHistoryId),   "
                + "        $(columns),             "
                + "        '$(channelName)',             "
                + "        $(txIdExpression),   "
                + "        $[sym.sync.table.prefix]_pkg.disable_node_id,     "
                + "        $(externalSelect),        "
                + "        CURRENT_TIMESTAMP   "
                + "      );   " + "    end if;   " + "  end;   ";
    }

    @Override
    protected String getUpdateTriggerTemplate() {
        return " create or replace trigger $(triggerName) after update on $(schemaName)$(tableName)  "
                + "  for each row begin  "
                + "    declare   "
                + "      var_row_data long;  "
                + "      var_old_data long;  "
                + "    begin  "
                + "    if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then  "
                + "      select $(columns) into var_row_data from dual;  "
                + "      select $(oldColumns) into var_old_data from dual;  "
                + "      if $(dataHasChangedCondition) then   "
                + "        insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)  "
                + "        values(  " + "          '$(targetTableName)',  " + "          'U',  "
                + "          $(triggerHistoryId),  " + "          $(oldKeys),  "
                + "          var_row_data,  " + "          var_old_data,  "
                + "          '$(channelName)',    " + "          $(txIdExpression),  "
                + "          $[sym.sync.table.prefix]_pkg.disable_node_id,     "
                + "          $(externalSelect),                                            "
                + "          CURRENT_TIMESTAMP  " + "        );  " + "      end if;  "
                + "    end if;  " + "  end;  " + "  end;  ";
    }

    @Override
    protected String getDeleteTriggerTemplate() {
        return "create or replace trigger  $(triggerName) after delete on $(schemaName)$(tableName) "
                + " for each row begin "
                + "   if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then "
                + "     insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) "
                + "     values( " + "       '$(targetTableName)', " + "       'D', "
                + "       $(triggerHistoryId), " + "       $(oldKeys), " + "       $(oldColumns), "
                + "       '$(channelName)',   " + "       $(txIdExpression), "
                + "       $[sym.sync.table.prefix]_pkg.disable_node_id,  "
                + "       $(externalSelect),       " + "       CURRENT_TIMESTAMP " + "     ); "
                + "   end if; " + " end; ";
    }

    @Override
    protected String getTransactionTriggerExpression() {
        return  "$(prefixName)_transaction_id()";
    }

    @Override
    protected String getDataHasChangedCondition() {
        return "var_row_data != var_old_data";
    }

    @Override
    protected String getSyncTriggersExpression() {
        return "$(prefixName)__trigger_disabled() is null";
    }

}
