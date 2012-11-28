package org.jumpmind.symmetric.db.sqlite;

import java.util.HashMap;

import org.jumpmind.symmetric.db.AbstractSymmetricDialect;
import org.jumpmind.symmetric.db.AbstractTriggerTemplate;

public class SqliteTriggerTemplate extends AbstractTriggerTemplate {

    public SqliteTriggerTemplate(AbstractSymmetricDialect symmetricDialect) {
        super(symmetricDialect);

        // formatter:off
        triggerConcatCharacter = "||";
        newTriggerValue = "new";
        oldTriggerValue = "old";
        stringColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else '\"' || replace(replace($(tableAlias).$(columnName),'\\','\\\\'),'\"','\\\"') || '\"' end";
        clobColumnTemplate = stringColumnTemplate;
        emptyColumnTemplate = "";
        numberColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else ('\"' || cast($(tableAlias).$(columnName) as varchar) || '\"') end";
        datetimeColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' else ('\"' || convert(varchar,$(tableAlias).$(columnName),121) || '\"') end";
        booleanColumnTemplate = "case when $(tableAlias).$(columnName) is null then '' when $(tableAlias).$(columnName) = 1 then '\"1\"' else '\"0\"' end";
        blobColumnTemplate = "case when $(origTableAlias).$(columnName) is null then '' else '\"' || replace(replace(hex($(origTableAlias).$(columnName)),'\\','\\\\'),'\"','\\\"') || '\"' end ";

        sqlTemplates = new HashMap<String, String>();
        sqlTemplates
                .put("insertTriggerTemplate",
                        "create trigger $(triggerName) after insert on $(schemaName)$(tableName)    \n"
                                + "for each row     \n"
                                + "  when $(syncOnInsertCondition)    \n"
                                + "  begin    \n"
                                + "    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time)    \n"
                                + "    values(    \n" + "      '$(targetTableName)',    \n" + "      'I',    \n"
                                + "      $(triggerHistoryId),                                          \n"
                                + "      $(columns),    \n" + "      '$(channelName)', null,null,    \n"
                                + "      $(externalSelect),    \n" + "     strftime('%Y-%m-%d %H:%M:%f','now','localtime')    \n" + "    );    \n"
                                + "end");

        sqlTemplates
                .put("updateTriggerTemplate",
                        "create trigger $(triggerName) after update on $(schemaName)$(tableName)   \n"
                                + "for each row    \n"
                                + "  when $(syncOnUpdateCondition)     \n"
                                + "  begin   \n"
                                + "    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, row_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)   \n"
                                + "    values(   \n" + "      '$(targetTableName)',   \n" + "      'U',   \n"
                                + "      $(triggerHistoryId),   \n" + "      $(oldKeys),   \n"
                                + "      $(columns),   \n" + "      $(oldColumns),   \n"
                                + "      '$(channelName)', null,null,   \n" + "      $(externalSelect),   \n"
                                + "      strftime('%Y-%m-%d %H:%M:%f','now','localtime')  \n" + "    );   \n" + "end  ");

        sqlTemplates
                .put("deleteTriggerTemplate",
                        "create trigger $(triggerName) after delete on $(schemaName)$(tableName)    \n"
                                + "for each row     \n"
                                + "  when $(syncOnDeleteCondition)    \n"
                                + "  begin    \n"
                                + "    insert into $(defaultCatalog)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time)    \n"
                                + "    values(    \n" + "      '$(targetTableName)',    \n" + "      'D',    \n"
                                + "      $(triggerHistoryId),    \n" + "      $(oldKeys),    \n"
                                + "       $(oldColumns),    \n" + "      '$(channelName)', null,null,    \n"
                                + "      $(externalSelect),    \n" + "     strftime('%Y-%m-%d %H:%M:%f','now','localtime') \n" + "    );    \n"
                                + "end");

        sqlTemplates.put("initialLoadSqlTemplate",
                "select $(columns) from $(schemaName)$(tableName) t where $(whereClause)");

        // formatter:on
    }
}
