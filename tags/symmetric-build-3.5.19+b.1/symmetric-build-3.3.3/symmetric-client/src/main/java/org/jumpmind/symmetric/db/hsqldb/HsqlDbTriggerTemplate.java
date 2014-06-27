package org.jumpmind.symmetric.db.hsqldb;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class HsqlDbTriggerTemplate extends AbstractTriggerTemplate {

    public HsqlDbTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect); 
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        numberColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',cast($(tableAlias)\"$(columnName)\" as varchar(50))),''\"'') end" ;
        datetimeColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        clobColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        blobColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' else concat(concat(''\"'',replace(replace($(tableAlias)\"$(columnName)\",''\\'',''\\\\''),''\"'',''\\\"'')),''\"'') end" ;
        booleanColumnTemplate = "case when $(tableAlias)\"$(columnName)\" is null then '''' when $(tableAlias)\"$(columnName)\" then ''\"1\"'' else ''\"0\"'' end" ;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "" ;
        oldTriggerValue = "" ;
        oldColumnPrefix = "OLD_" ;
        newColumnPrefix = "NEW_" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL LONGVARCHAR, INSERT_DATA_SQL LONGVARCHAR);                                                                                                           " + 
"                                INSERT INTO $(triggerName)_CONFIG values(                                                                                                                              " + 
"                                'select count(*) from $(virtualOldNewTable) where $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition)',                                                       " + 
"                                'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                  (select ''$(targetTableName)'',''I'',$(triggerHistoryId),$(columns), ''$(channelName)'', $(txIdExpression), sym_get_session(''node_value''), $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))' " + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(triggerName) AFTER INSERT ON $(tableName) FOR EACH ROW QUEUE 0 CALL \"org.jumpmind.symmetric.db.hsqldb.HsqlDbTrigger\";                               " );
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL LONGVARCHAR, INSERT_DATA_SQL LONGVARCHAR);                                                                                                           " + 
"                                INSERT INTO $(triggerName)_CONFIG values(                                                                                                                              " + 
"                                  'select count(*) from $(virtualOldNewTable) where $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition)',                                                     " + 
"                                  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    (select ''$(targetTableName)'',''U'',$(triggerHistoryId),$(oldKeys),$(columns),$(oldColumns), ''$(channelName)'', $(txIdExpression), sym_get_session(''node_value''), $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'" + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(triggerName) AFTER UPDATE ON $(tableName) FOR EACH ROW QUEUE 0 CALL \"org.jumpmind.symmetric.db.hsqldb.HsqlDbTrigger\";                               " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TABLE $(triggerName)_CONFIG (CONDITION_SQL LONGVARCHAR, INSERT_DATA_SQL LONGVARCHAR);                                                                                                           " + 
"                                INSERT INTO $(triggerName)_CONFIG values(                                                                                                                              " + 
"                                  'select count(*) from $(virtualOldNewTable) where $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition)',                                                     " + 
"                                  'insert into $(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)" + 
"                                    (select ''$(targetTableName)'',''D'',$(triggerHistoryId),$(oldKeys),$(oldColumns),''$(channelName)'', $(txIdExpression), sym_get_session(''node_value''), $(externalSelect), CURRENT_TIMESTAMP from $(virtualOldNewTable))'" + 
"                                );                                                                                                                                                                     " + 
"                                CREATE TRIGGER $(triggerName) AFTER DELETE ON $(tableName) FOR EACH ROW QUEUE 0 CALL \"org.jumpmind.symmetric.db.hsqldb.HsqlDbTrigger\";                               " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}