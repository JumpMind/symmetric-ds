package org.jumpmind.symmetric.db.db2;

import org.jumpmind.symmetric.db.ISymmetricDialect;

public class Db2As400TriggerTemplate extends Db2TriggerTemplate {

    public Db2As400TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             \n"+
"                                AFTER UPDATE ON $(schemaName)$(tableName)                                                                                                                              \n"+
"                                REFERENCING OLD AS OLD NEW AS NEW                                                                                                                                      \n"+
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               \n"+
"                                BEGIN ATOMIC                                                                                                                                                           \n"+
"                                    IF $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then                                                                                             \n"+
"                                            INSERT into $(defaultSchema)$(prefixName)_data                                                                                                             \n"+
"                                                (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)         \n"+
"                                            VALUES('$(targetTableName)', 'U', $(triggerHistoryId),                                                                                                     \n"+
"                                                $(oldKeys),                                                                                                                                            \n"+
"                                                $(columns),                                                                                                                                          \n"+
"                                                $(oldColumns),                                                                                                                                          \n"+
"                                                $(channelExpression),                                                                                                                                      \n"+
"                                                $(txIdExpression),                                                                                                                                     \n"+
"                                                $(sourceNodeExpression),                                                                                                                               \n"+
"                                                $(externalSelect),                                                                                                                                     \n"+
"                                                CURRENT_TIMESTAMP);                                                                                                                                    \n"+
"                                    END IF;                                                                                                                                                            \n"+
"                                    $(custom_on_update_text)                                                                                                                                           \n"+
"                                END                                                                                                                                                                    " );

    }
    
    public boolean useTriggerTemplateForColumnTemplatesDuringInitialLoad() {
        return false;
    }

}
