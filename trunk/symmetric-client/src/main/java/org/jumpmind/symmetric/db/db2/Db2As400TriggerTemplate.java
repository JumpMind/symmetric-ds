package org.jumpmind.symmetric.db.db2;

import org.jumpmind.symmetric.db.ISymmetricDialect;

public class Db2As400TriggerTemplate extends Db2TriggerTemplate {

    public Db2As400TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             " +
"                                AFTER UPDATE ON $(schemaName)$(tableName)                                                                                                                              " +
"                                REFERENCING OLD AS OLD NEW AS NEW                                                                                                                                      " +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " +
"                                BEGIN ATOMIC                                                                                                                                                           " +
"                                    IF $(syncOnIncomingBatchCondition) then                                                                                               " +
"                                        IF $(dataHasChangedCondition) THEN                                                                                                                             " +
"                                            INSERT into $(defaultSchema)$(prefixName)_data                                                                                                             " +
"                                                (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)         " +
"                                            VALUES('$(targetTableName)', 'U', $(triggerHistoryId),                                                                                                     " +
"                                                $(oldKeys),                                                                                                                                            " +
"                                                $(columns),                                                                                                                                          " +
"                                                $(oldColumns),                                                                                                                                          " +
"                                                $(channelExpression),                                                                                                                                      " +
"                                                $(txIdExpression),                                                                                                                                     " +
"                                                $(sourceNodeExpression),                                                                                                                               " +
"                                                $(externalSelect),                                                                                                                                     " +
"                                                CURRENT_TIMESTAMP);                                                                                                                                    " +
"                                        END IF;                                                                                                                                                        " +
"                                    END IF;                                                                                                                                                            " +
"                                    $(custom_on_update_text)                                                                                                                                           " +
"                                END                                                                                                                                                                    " );

    }
    
    public boolean useTriggerTemplateForColumnTemplatesDuringInitialLoad() {
        return false;
    }

}
