package org.jumpmind.symmetric.core.db.h2;

import java.util.HashMap;
import java.util.Map;

import org.jumpmind.symmetric.core.common.BinaryEncoding;
import org.jumpmind.symmetric.core.db.IDbDialect;
import org.jumpmind.symmetric.core.db.AbstractDataCaptureBuilder;
import org.jumpmind.symmetric.core.db.SqlConstants;
import org.jumpmind.symmetric.core.process.sql.TableToExtract;

public class H2DataCaptureBuilder extends AbstractDataCaptureBuilder {

    public H2DataCaptureBuilder(IDbDialect dbPlatform) {
        super(dbPlatform);
    }

    public BinaryEncoding getBinaryEncoding() {
        return BinaryEncoding.BASE64;
    }

    @Override
    public String createTableExtractSql(TableToExtract tableToExtract,
            Map<String, String> replacementTokens, boolean supportsBigLobs) {
        return super.createTableExtractSql(tableToExtract, replacementTokens, supportsBigLobs)
                .replace("''", "'");
    }

    @Override
    protected String getTableExtractSqlTableAlias() {
        return "t.";
    }

    @Override
    protected String getClobColumnTemplate() {
        return getStringColumnTemplate();
    }

    @Override
    protected String getNewTriggerValue() {
        return "";
    }

    @Override
    protected String getOldTriggerValue() {
        return "";
    }

    @Override
    protected String getBlobColumnTemplate() {
        return "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||replace(replace($[sym.sync.table.prefix]_BASE64_ENCODE($(tableAlias)\"$(columnName)\"),''\\'',''\\\\''),''\"'',''\\\"'')||''\"'' end";
    }
    
    @Override
    protected String getDateTimeWithTimeZoneTemplate() {
        return null;
    }

    @Override
    protected String getWrappedBlobColumnTemplate() {
        return null;
    }

    @Override
    protected Map<String, String> getFunctionTemplatesToInstall() {
        Map<String, String> functionTemplatesToInstall = new HashMap<String, String>();
        functionTemplatesToInstall
                .put("BASE64_ENCODE",
                        "CREATE ALIAS IF NOT EXISTS $(functionName) for \"org.jumpmind.symmetric.core.db.EmbeddedDbFunctions.encodeBase64\";");
        return functionTemplatesToInstall;
    }

    @Override
    protected String getFunctionInstalledSqlTemplate() {
        return "select count(*) from INFORMATION_SCHEMA.FUNCTION_ALIASES where ALIAS_NAME='$(functionName)'";
    }
    
    @Override
    protected String getStringColumnTemplate() {
        return "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')||''\"'' end";
    }

    @Override
    protected String getNumberColumnTemplate() {
        return "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||cast($(tableAlias)\"$(columnName)\" as varchar(50))||''\"'' end";
    }

    @Override
    protected String getDateTimeColumnTemplate() {
        return "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||formatdatetime($(tableAlias)\"$(columnName)\", ''yyyy-MM-dd HH:mm:ss.S'')||''\"'' end";
    }

    @Override
    protected String getBooleanColumnTemplate() {
        return "case when $(tableAlias)\"$(columnName)\" is null then '''' when $(tableAlias)\"$(columnName)\" then ''\"1\"'' else ''\"0\"'' end";
    }

    @Override
    protected String getTimeColumnTemplate() {
        return null;
    }

    @Override
    protected String getDateColumnTemplate() {
        return null;
    }

    @Override
    protected String getTriggerConcatCharacter() {
        return "||";
    }

    @Override
    protected String getOldColumnPrefix() {
        return "OLD_";
    }

    @Override
    protected String getNewColumnPrefix() {
        return "NEW_";
    }

    @Override
    protected String getInsertTriggerTemplate() {
        return "CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL CLOB, INSERT_DATA_SQL CLOB);"
                + "INSERT INTO $(triggerName)_CONFIG values(    "
                + "'select count(*) from $(virtualOldNewTable) where $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)',"
                + "'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)"
                + "  (select ''$(targetTableName)'',''I'',$(triggerHistoryId),$(columns), ''$(channelName)'', $(txIdExpression), @node_value, $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'"
                + ");"
                + "CREATE TRIGGER $(triggerName) AFTER INSERT ON $(tableName) FOR EACH ROW CALL \"org.jumpmind.symmetric.db.h2.H2Trigger\";";
    }

    @Override
    protected String getUpdateTriggerTemplate() {
        return "CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL CLOB, INSERT_DATA_SQL CLOB);"
                + "INSERT INTO $(triggerName)_CONFIG values(    "
                + "  'select count(*) from $(virtualOldNewTable) where $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)', "
                + "  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)"
                + "    (select ''$(targetTableName)'',''U'',$(triggerHistoryId),$(oldKeys),$(columns),$(oldColumns), ''$(channelName)'', $(txIdExpression), @node_value, $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'"
                + ");"
                + "CREATE TRIGGER $(triggerName) AFTER UPDATE ON $(tableName) FOR EACH ROW CALL \"org.jumpmind.symmetric.db.h2.H2Trigger\";";
    }

    @Override
    protected String getDeleteTriggerTemplate() {
        return "CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL CLOB, INSERT_DATA_SQL CLOB);"
                + "INSERT INTO $(triggerName)_CONFIG values(    "
                + "  'select count(*) from $(virtualOldNewTable) where $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)', "
                + "  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)"
                + "    (select ''$(targetTableName)'',''D'',$(triggerHistoryId),$(oldKeys),$(oldColumns),''$(channelName)'', $(txIdExpression), @node_value, $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'"
                + ");"
                + "CREATE TRIGGER $(triggerName) AFTER DELETE ON $(tableName) FOR EACH ROW CALL \"org.jumpmind.symmetric.db.h2.H2Trigger\";";

    }

    @Override
    protected String getTransactionTriggerExpression() {
        return "TRANSACTION_ID()";
    }

    @Override
    protected String preProcessTriggerSqlClause(String sqlClause) {
        sqlClause = sqlClause.replace("$(newTriggerValue).", "$(newTriggerValue)");
        sqlClause = sqlClause.replace("$(oldTriggerValue).", "$(oldTriggerValue)");
        sqlClause = sqlClause.replace("$(curTriggerValue).", "$(curTriggerValue)");
        return sqlClause.replace("'", "''");
    }

    @Override
    protected String getDataHasChangedCondition() {
        return SqlConstants.ALWAYS_TRUE_CONDITION;
    }

    @Override
    protected String getSyncTriggersExpression() {
        return " @sync_prevented is null ";
    }

}
