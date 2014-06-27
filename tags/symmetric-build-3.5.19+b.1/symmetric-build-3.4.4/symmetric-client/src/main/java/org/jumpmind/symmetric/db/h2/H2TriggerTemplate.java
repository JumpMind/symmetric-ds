package org.jumpmind.symmetric.db.h2;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class H2TriggerTemplate extends AbstractTriggerTemplate {

    public H2TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        
        emptyColumnTemplate = "''''" ;
        stringColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')||''\"'' end" ;
        numberColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||cast($(tableAlias)\"$(columnName)\" as varchar(50))||''\"'' end" ;
        datetimeColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||formatdatetime($(tableAlias)\"$(columnName)\", ''yyyy-MM-dd HH:mm:ss.S'')||''\"'' end" ;
        clobColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')||''\"'' end" ;
        blobColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else ''\"''||replace(replace(sym_BASE64_ENCODE($(tableAlias)\"$(columnName)\"),''\\'',''\\\\''),''\"'',''\\\"'')||''\"'' end" ;
        booleanColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' when $(tableAlias)\"$(columnName)\" then ''\"1\"'' else ''\"0\"'' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "" ;
        oldTriggerValue = "" ;
        oldColumnPrefix = "OLD_" ;
        newColumnPrefix = "NEW_" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TABLE $(schemaName)$(triggerName)_CONFIG (CONDITION_SQL CLOB, INSERT_DATA_SQL CLOB);                                                                                                                         " + 
"                                INSERT INTO $(schemaName)$(triggerName)_CONFIG values(                                                                                                                              " + 
"                                'select count(*) from $(virtualOldNewTable) where $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)',                                                       " + 
"                                'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                  (select ''$(targetTableName)'',''I'',$(triggerHistoryId),$(columns), ''$(channelName)'', $(txIdExpression), @node_value, $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))' " + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(schemaName)$(triggerName) AFTER INSERT ON $(schemaName)$(tableName) FOR EACH ROW CALL \"org.jumpmind.symmetric.db.h2.H2Trigger\";                                               " );
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TABLE $(schemaName)$(triggerName)_CONFIG (CONDITION_SQL CLOB, INSERT_DATA_SQL CLOB);                                                                                                                         " + 
"                                INSERT INTO $(schemaName)$(triggerName)_CONFIG values(                                                                                                                              " + 
"                                  'select count(*) from $(virtualOldNewTable) where $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)',                                                     " + 
"                                  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    (select ''$(targetTableName)'',''U'',$(triggerHistoryId),$(oldKeys),$(columns),$(oldColumns), ''$(channelName)'', $(txIdExpression), @node_value, $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'" + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(schemaName)$(triggerName) AFTER UPDATE ON $(schemaName)$(tableName) FOR EACH ROW CALL \"org.jumpmind.symmetric.db.h2.H2Trigger\";                                               " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TABLE $(schemaName)$(triggerName)_CONFIG (CONDITION_SQL CLOB, INSERT_DATA_SQL CLOB);                                                                                                                         " + 
"                                INSERT INTO $(schemaName)$(triggerName)_CONFIG values(                                                                                                                              " + 
"                                  'select count(*) from $(virtualOldNewTable) where $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)',                                                     " + 
"                                  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    (select ''$(targetTableName)'',''D'',$(triggerHistoryId),$(oldKeys),$(oldColumns),''$(channelName)'', $(txIdExpression), @node_value, $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'" + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(schemaName)$(triggerName) AFTER DELETE ON $(schemaName)$(tableName) FOR EACH ROW CALL \"org.jumpmind.symmetric.db.h2.H2Trigger\";                                               " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}