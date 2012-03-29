package org.jumpmind.symmetric.core.db.postgres;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.db.AbstractDataCaptureBuilder;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.SqlConstants;

public class PostgresDataCaptureBuilder extends AbstractDataCaptureBuilder {

    static final String TRANSACTION_ID_EXPRESSION = "txid_current()";

    protected boolean supportsTransactionId = false;
    protected String transactionIdExpression = "null";

    public PostgresDataCaptureBuilder(IDbDialect dbDialect) {
        super(dbDialect);
        int majorVersion = dbDialect.getSqlTemplate().getDatabaseMajorVersion();
        int minorVersion = dbDialect.getSqlTemplate().getDatabaseMinorVersion();
        if (majorVersion > 8 || (majorVersion == 8 && minorVersion >= 3)) {
            log.debug("Transaction id capture is enabled.");
            supportsTransactionId = true;
            transactionIdExpression = TRANSACTION_ID_EXPRESSION;
        }
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    @Override
    protected String getClobColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end";
    }

    @Override
    protected String getNewTriggerValue() {
        return "new";
    }

    @Override
    protected String getOldTriggerValue() {
        return "old";
    }

    @Override
    protected String getBlobColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || encode($(tableAlias).\"$(columnName)\", 'base64') || '\"' end";
    }

    @Override
    protected String getWrappedBlobColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || $(defaultSchema)$(prefixName)_largeobject($(tableAlias).\"$(columnName)\") || '\"' end";
    }

    @Override
    protected Map<String, String> getFunctionTemplatesToInstall() {
        Map<String, String> functionTemplatesToInstall = new HashMap<String, String>();
        functionTemplatesToInstall
                .put("triggers_disabled",
                        "CREATE or REPLACE FUNCTION $(defaultSchema)$(functionName)() RETURNS INTEGER AS $$   "
                                + "   DECLARE                                                                                      "
                                + "     triggerDisabled INTEGER;                                                                   "
                                + "   BEGIN                                                                                        "
                                + "     select current_setting('symmetric.triggers_disabled') into triggerDisabled;                "
                                + "     return triggerDisabled;                                                                    "
                                + "   EXCEPTION WHEN OTHERS THEN                                                                   "
                                + "     return 0;                                                                                  "
                                + "   END;                                                                                         "
                                + "   $$ LANGUAGE plpgsql;");

        functionTemplatesToInstall
                .put("node_disabled",
                        "CREATE or REPLACE FUNCTION $(defaultSchema)$(functionName)() RETURNS VARCHAR AS $$   "
                                + "  DECLARE                                                                                                     "
                                + "    nodeId VARCHAR(50);                                                                                       "
                                + "  BEGIN                                                                                                       "
                                + "    select current_setting('symmetric.node_disabled') into nodeId;                                            "
                                + "    return nodeId;                                                                                            "
                                + "  EXCEPTION WHEN OTHERS THEN                                                                                  "
                                + "    return '';                                                                                                "
                                + "  END;                                                                                                        "
                                + "  $$ LANGUAGE plpgsql;");
        functionTemplatesToInstall
                .put("largeobject",
                        "CREATE OR REPLACE FUNCTION $(defaultSchema)$(functionName)(objectId oid) RETURNS text AS $$         "
                                + "  DECLARE                                                                                                                  "
                                + "    encodedBlob text;                                                                                                      "
                                + "    encodedBlobPage text;                                                                                                  "
                                + "  BEGIN                                                                                                                    "
                                + "    encodedBlob := '';                                                                                                     "
                                + "    FOR encodedBlobPage IN SELECT encode(data, 'escape')                                                                   "
                                + "    FROM pg_largeobject WHERE loid = objectId ORDER BY pageno LOOP                                                         "
                                + "      encodedBlob := encodedBlob || encodedBlobPage;                                                                       "
                                + "    END LOOP;                                                                                                              "
                                + "    RETURN encode(decode(encodedBlob, 'escape'), 'base64');                                                                "
                                + "  EXCEPTION WHEN OTHERS THEN                                                                                               "
                                + "    RETURN '';                                                                                                             "
                                + "  END                                                                                                                      "
                                + "  $$ LANGUAGE plpgsql;");
        return functionTemplatesToInstall;
    }

    @Override
    protected String getFunctionInstalledSqlTemplate() {
        return "select count(*) from information_schema.routines where routine_name = '$(functionName)' and specific_schema = '$(defaultSchema)'";
    }

    @Override
    protected String getStringColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end";
    }

    @Override
    protected String getXmlColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end";
    }

    @Override
    protected String getArrayColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as varchar),$$\\$$,$$\\\\$$),'\"',$$\\\"$$) || '\"' end";
    }

    @Override
    protected String getNumberColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast($(tableAlias).\"$(columnName)\" as varchar) || '\"' end";
    }

    @Override
    protected String getDateTimeColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.MS') || '\"' end";
    }
    
    @Override
    protected String getDateTimeWithTimeZoneTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || to_char($(tableAlias).\"$(columnName)\", 'YYYY-MM-DD HH24:MI:SS.US ')||lpad(cast(extract(timezone_hour from $(tableAlias).\"$(columnName)\") as varchar),2,'0')||':'||lpad(cast(extract(timezone_minute from $(tableAlias).\"$(columnName)\") as varchar), 2, '0') || '\"' end";
    }
    

    @Override
    protected String getBooleanColumnTemplate() {
        return "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" then '\"1\"' else '\"0\"' end";
    }

    @Override
    protected String getTimeColumnTemplate() {
        return getDateTimeColumnTemplate();
    }

    @Override
    protected String getDateColumnTemplate() {
        return getDateTimeColumnTemplate();
    }

    @Override
    protected String getTriggerConcatCharacter() {
        return "||";
    }

    @Override
    protected String getPostInsertTriggerTemplate() {
        return "create trigger $(triggerName) after insert on $(schemaName)$(tableName) "
                + " for each row execute procedure $(schemaName)f$(triggerName)();";
    }

    @Override
    protected String getPostUpdateTriggerTemplate() {
        return "create trigger $(triggerName) after update on $(schemaName)$(tableName) "
                + " for each row execute procedure $(schemaName)f$(triggerName)();";
    }

    @Override
    protected String getPostDeleteTriggerTemplate() {
        return "create trigger $(triggerName) after delete on $(schemaName)$(tableName) "
                + " for each row execute procedure $(schemaName)f$(triggerName)();";
    }

    @Override
    protected String getInsertTriggerTemplate() {
        return "create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                    "
                + "            begin                                                                                                                                    "
                + "              if $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                   "
                + "                insert into $(defaultSchema)$(prefixName)_data                                                                                       "
                + "                (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)          "
                + "                values(                                                                                                                              "
                + "                  '$(targetTableName)',                                                                                                              "
                + "                  'I',                                                                                                                               "
                + "                  $(triggerHistoryId),                                                                                                               "
                + "                  $(columns),                                                                                                                        "
                + "                  '$(channelName)',                                                                                                                  "
                + "                  $(txIdExpression),                                                                                                                 "
                + "                  $(defaultSchema)$(prefixName)_node_disabled(),                                                                                     "
                + "                  $(externalSelect),                                                                                                                 "
                + "                  CURRENT_TIMESTAMP                                                                                                                  "
                + "                );                                                                                                                                   "
                + "              end if;                                                                                                                                "
                + "              return null;                                                                                                                           "
                + "            end;                                                                                                                                     "
                + "            $function$ language plpgsql;";
    }

    @Override
    protected String getUpdateTriggerTemplate() {
        return "create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                             "
                + "           begin                                                                                                                                             "
                + "             if $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                            "
                + "               insert into $(defaultSchema)$(prefixName)_data                                                                                                "
                + "               (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)"
                + "               values(                                                                                                                                       "
                + "                 '$(targetTableName)',                                                                                                                       "
                + "                 'U',                                                                                                                                        "
                + "                 $(triggerHistoryId),                                                                                                                        "
                + "                 $(oldKeys),                                                                                                                                 "
                + "                 $(columns),                                                                                                                                 "
                + "                 $(oldColumns),                                                                                                                              "
                + "                 '$(channelName)',                                                                                                                           "
                + "                 $(txIdExpression),                                                                                                                          "
                + "                 $(defaultSchema)$(prefixName)_node_disabled(),                                                                                              "
                + "                 $(externalSelect),                                                                                                                          "
                + "                 CURRENT_TIMESTAMP                                                                                                                           "
                + "               );                                                                                                                                            "
                + "             end if;                                                                                                                                         "
                + "             return null;                                                                                                                                    "
                + "           end;                                                                                                                                              "
                + "           $function$ language plpgsql;";
    }

    @Override
    protected String getDeleteTriggerTemplate() {
        return "create or replace function $(schemaName)f$(triggerName)() returns trigger as $function$                                                                         "
                + "        begin                                                                                                                                         "
                + "          if $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                        "
                + "            insert into $(defaultSchema)$(prefixName)_data                                                                                            "
                + "            (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)      "
                + "            values(                                                                                                                                   "
                + "              '$(targetTableName)',                                                                                                                   "
                + "              'D',                                                                                                                                    "
                + "              $(triggerHistoryId),                                                                                                                    "
                + "              $(oldKeys),                                                                                                                             "
                + "              $(oldColumns),                                                                                                                          "
                + "              '$(channelName)',                                                                                                                       "
                + "              $(txIdExpression),                                                                                                                      "
                + "              $(defaultSchema)$(prefixName)_node_disabled(),                                                                                          "
                + "              $(externalSelect),                                                                                                                      "
                + "              CURRENT_TIMESTAMP                                                                                                                       "
                + "            );                                                                                                                                        "
                + "          end if;                                                                                                                                     "
                + "          return null;                                                                                                                                "
                + "        end;                                                                                                                                          "
                + "        $function$ language plpgsql;";
    }

    @Override
    protected String getTransactionTriggerExpression() {
        return transactionIdExpression;
    }

    @Override
    protected String getDataHasChangedCondition() {
        return SqlConstants.ALWAYS_TRUE_CONDITION;
    }

    @Override
    protected String getSyncTriggersExpression() {
        return "$(defaultSchema)" + dbDialect.getParameters().getTablePrefix()
                + "_triggers_disabled() = 0";
    }

}
