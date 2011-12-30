package org.jumpmind.symmetric.db.db2;

import org.jumpmind.symmetric.db.TriggerText;
import java.util.HashMap;

public class Db2zSeriesTriggerText extends TriggerText {

    public Db2zSeriesTriggerText() { 
        functionInstalledSql = "select count(*) from sysibm.sysfunctions where name = '$(functionName)'" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when length($(tableAlias).\"$(columnName)\") = 0 then '' else '\"' || replace(replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"') || '\"' end" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || STRIP(char($(tableAlias).\"$(columnName)\")) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || STRIP(char(year($(tableAlias).\"$(columnName)\")))||'-'||substr(digits(month($(tableAlias).\"$(columnName)\")),9)||'-'||substr(digits(day($(tableAlias).\"$(columnName)\")),9)||' '||substr(digits(hour($(tableAlias).\"$(columnName)\")),9)||':'||substr(digits(minute($(tableAlias).\"$(columnName)\")),9)||':'||substr(digits(second($(tableAlias).\"$(columnName)\")),9)||'.'||STRIP(char(microsecond($(tableAlias).\"$(columnName)\"))) || '\"' end" ;
        timeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || substr(digits(hour($(tableAlias).\"$(columnName)\")),9)||':'||substr(digits(minute($(tableAlias).\"$(columnName)\")),9)||':'||substr(digits(second($(tableAlias).\"$(columnName)\")),9)|| '\"' end" ;
        dateColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || STRIP(char(year($(tableAlias).\"$(columnName)\")))||'-'||substr(digits(month($(tableAlias).\"$(columnName)\")),9)||'-'||substr(digits(day($(tableAlias).\"$(columnName)\")),9)|| '\"' end" ;
        clobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || cast($(tableAlias).\"$(columnName)\" as varchar(32672)) || '\"' end" ;
        blobColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else hex(cast($(tableAlias).\"$(columnName)\" as varchar(16336) for bit data)) end" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = null;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = null;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                                                                                                                                                          " + 
"                                AFTER INSERT ON $(schemaName)$(tableName)                                                                                                                              " + 
"                                REFERENCING NEW AS NEW                                                                                                                                                 " + 
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " + 
"                                    WHEN ($(syncOnInsertCondition))                                                                                                                                    " + 
"                                        BEGIN ATOMIC                                                                                                                                                   " + 
"                                            INSERT into $(defaultSchema)$(prefixName)_data                                                                                                             " + 
"                                                (table_name, event_type, trigger_hist_id, row_data, create_time)                                                                                       " + 
"                                            VALUES('$(targetTableName)', 'I', $(triggerHistoryId),                                                                                                     " + 
"                                                $(columns),                                                                                                                                            " + 
"                                                CURRENT_TIMESTAMP);                                                                                                                                    " + 
"                                            INSERT into $(defaultSchema)$(prefixName)_data_event                                                                                                       " + 
"                                                (node_id, data_id, channel_id, transaction_id)                                                                                                         " + 
"                                            SELECT node_id, IDENTITY_VAL_LOCAL(), '$(channelName)',                                                                                                    " + 
"                                                $(txIdExpression) from $(prefixName)_node c                                                                                                            " + 
"                                                where (c.node_group_id = '$(targetGroupId)' and c.sync_enabled = 1) $(nodeSelectWhere);                                                                " + 
"                                        END;                                                                                                                                                           " );
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                                                                                                                                                          " + 
"                                AFTER UPDATE ON $(schemaName)$(tableName)                                                                                                                              " + 
"                                REFERENCING OLD AS OLD NEW AS NEW                                                                                                                                      " + 
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " + 
"                                WHEN ($(syncOnUpdateCondition))                                                                                                                                        " + 
"                                    BEGIN ATOMIC                                                                                                                                                       " + 
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 " + 
"                                            (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, create_time)                                                                        " + 
"                                        VALUES('$(targetTableName)', 'U', $(triggerHistoryId),                                                                                                         " + 
"                                            $(oldKeys),                                                                                                                                                " + 
"                                            $(columns),                                                                                                                                                " + 
"                                            $(oldColumns),                                                                                                                                             " + 
"                                            CURRENT_TIMESTAMP);                                                                                                                                        " + 
"                                        INSERT into $(defaultSchema)$(prefixName)_data_event                                                                                                           " + 
"                                            (node_id, data_id, channel_id, transaction_id)                                                                                                             " + 
"                                        SELECT node_id, IDENTITY_VAL_LOCAL(), '$(channelName)',                                                                                                        " + 
"                                            $(txIdExpression) from $(prefixName)_node c                                                                                                                " + 
"                                            where (c.node_group_id = '$(targetGroupId)' and c.sync_enabled = 1) $(nodeSelectWhere);                                                                    " + 
"                                END;                                                                                                                                                                   " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                                                                                                                                                          " + 
"                                AFTER DELETE ON $(schemaName)$(tableName)                                                                                                                              " + 
"                                REFERENCING OLD AS OLD                                                                                                                                                 " + 
"                                FOR EACH ROW MODE DB2SQL                                                                                                                                               " + 
"                                WHEN ($(syncOnDeleteCondition))                                                                                                                                        " + 
"                                    BEGIN ATOMIC                                                                                                                                                       " + 
"                                        INSERT into $(defaultSchema)$(prefixName)_data                                                                                                                 " + 
"                                            (table_name, event_type, trigger_hist_id, pk_data, create_time)                                                                                            " + 
"                                        VALUES ('$(targetTableName)', 'D', $(triggerHistoryId),                                                                                                        " + 
"                                            $(oldKeys),                                                                                                                                                " + 
"                                            CURRENT_TIMESTAMP);                                                                                                                                        " + 
"                                        INSERT into $(defaultSchema)$(prefixName)_data_event                                                                                                           " + 
"                                            (node_id, data_id, channel_id, transaction_id)                                                                                                             " + 
"                                        SELECT node_id, IDENTITY_VAL_LOCAL(), '$(channelName)',                                                                                                        " + 
"                                            $(txIdExpression) from $(prefixName)_node c                                                                                                                " + 
"                                            where (c.node_group_id = '$(targetGroupId)' and c.sync_enabled = 1) $(nodeSelectWhere);                                                                    " + 
"                                END;                                                                                                                                                                   " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

}