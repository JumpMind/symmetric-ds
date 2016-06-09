package org.jumpmind.symmetric.db.db2;

import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class Db2As400TriggerTemplate extends Db2TriggerTemplate {

    public Db2As400TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"') || '\"' end" ;
        String castClobTo = symmetricDialect.getParameterService().getString(ParameterConstants.AS400_CAST_CLOB_TO, "DCLOB");
        
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || replace(replace(cast($(tableAlias).\"$(columnName)\" as "+castClobTo+"),'\\','\\\\'),'\"','\\\"') || '\"' end" ;

        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             " +
"                                AFTER INSERT ON $(schemaName)$(tableName)                                                                                                                              " +
"                                REFERENCING NEW AS NEW                                                                                                                                                 " +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " +
"                                BEGIN ATOMIC                                                                                                                                                           " +
"                                    IF $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then                                                                                               " +
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 " +
"                                            (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)                                " +
"                                        VALUES('$(targetTableName)', 'I', $(triggerHistoryId),                                                                                                         " +
"                                            $(columns),                                                                                                                                                " +
"                                            $(channelExpression), $(txIdExpression), $(sourceNodeExpression),                                                                                          " +
"                                            $(externalSelect),                                                                                                                                         " +
"                                            CURRENT_TIMESTAMP);                                                                                                                                        " +
"                                    END IF;                                                                                                                                                            " +
"                                    $(custom_on_insert_text)                                                                                                                                           " +
"                                END                                                                                                                                                                    " );

        
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
        
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(schemaName)$(triggerName)                                                                                                                                                             " +
"                                AFTER DELETE ON $(schemaName)$(tableName)                                                                                                                              " +
"                                REFERENCING OLD AS OLD                                                                                                                                                 " +
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " +
"                                BEGIN ATOMIC                                                                                                                                                           " +
"                                    IF $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then                                                                                               " +
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 " +
"                                            (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)                       " +
"                                        VALUES ('$(targetTableName)', 'D', $(triggerHistoryId),                                                                                                        " +
"                                            $(oldKeys),                                                                                                                                                " +
"                                            $(oldColumns),                                                                                                                                             " +
"                                            $(channelExpression),                                                                                                                                          " +
"                                            $(txIdExpression),                                                                                                                                         " +
"                                            $(sourceNodeExpression),                                                                                                                                   " +
"                                            $(externalSelect),                                                                                                                                         " +
"                                            CURRENT_TIMESTAMP);                                                                                                                                        " +
"                                    END IF;                                                                                                                                                            " +
"                                    $(custom_on_delete_text)                                                                                                                                           " +
"                                END                                                                                                                                                                    " );
        
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );

    }
    
    public boolean useTriggerTemplateForColumnTemplatesDuringInitialLoad() {
        return false;
    }

}
