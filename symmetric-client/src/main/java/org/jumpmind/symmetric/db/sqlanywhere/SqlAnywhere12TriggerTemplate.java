package org.jumpmind.symmetric.db.sqlanywhere;

import org.jumpmind.symmetric.db.ISymmetricDialect;

public class SqlAnywhere12TriggerTemplate extends SqlAnywhereTriggerTemplate {

    public SqlAnywhere12TriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        sqlTemplates.put("insertTriggerTemplate",
                "create or replace trigger $(triggerName) after insert order 10000 "
                        + "on $(schemaName)$(tableName) referencing new as inserted                                                                                                                                "
                        +
                        "                                begin                                                                                                                                                                  "
                        +
                        "                                  declare @DataRow varchar(16384);                                                                                                                                      "
                        +
                        "                                  $(declareNewKeyVariables)                                                                                                                                            "
                        +
                        "                                  declare @ChannelId varchar(20);                                                                                                                                     "
                        + 
                        "                                  declare @err_notfound EXCEPTION FOR SQLSTATE VALUE '02000';                                                                                                    "
                        +
                        "                                  $(custom_before_insert_text) \n" +
                        "                                  if ($(syncOnIncomingBatchCondition)) then begin                                                                                                                           "
                        +
                        "                                    declare DataCursor cursor for                                                                                                                                      "
                        +
                        "                                    $(if:containsBlobClobColumns)                                                                                                                                      "
                        +
                        "                                       select $(columns) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition);"
                        +
                        "                                    $(else:containsBlobClobColumns)                                                                                                                                    "
                        +
                        "                                       select $(columns) $(newKeyNames), $(channelExpression) from inserted where $(syncOnInsertCondition);                                                                                  "
                        +
                        "                                    $(end:containsBlobClobColumns)                                                                                                                                     "
                        +
                        "                                       open DataCursor;"
                        + 
                        "                                       LoopGetRow:"
                        + 
                        "                                       loop                                                                                                          "
                        +
                        "                                           fetch next DataCursor into @DataRow $(newKeyVariables), @ChannelId;                                                                                                   "
                        +
                        "                                           if SQLSTATE = @err_notfound then"
                        + 
                        "                                               leave LoopGetRow"
                        + 
                        "                                           end if;                                                                                                                                  "
                        +
                        "                                           insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time) "
                        +
                        "                                             values('$(targetTableName)','I', $(triggerHistoryId), @DataRow, @ChannelId, $(txIdExpression), $(defaultCatalog)$(defaultSchema)$(prefixName)_node_disabled(0), $(externalSelect), getdate());                                  "
                        +
                        "                                       end loop LoopGetRow;                                                                                                 "
                        +
                        "                                       close DataCursor;                                                                                                                                                "
                        +
                        "                                  end;                                                                                                                                                                 "
                        +
                        "                                  $(custom_on_insert_text)  "
                        + 
                        "                                   end if;                                                                                                                                         "
                        +
                        "                                end;                                                                                                                                                                  ");
        sqlTemplates.put("updateTriggerTemplate",
                "create or replace trigger $(triggerName) after update order 10000 on $(schemaName)$(tableName) "
                        + 
                        "                                   referencing old as deleted new as inserted "
                        +
                        "                                begin                                                                                                                                                                  "
                        +
                        "                                  declare @DataRow varchar(16384);                                                                                                                                      "
                        +
                        "                                  declare @OldPk varchar(2000);                                                                                                                                         "
                        +
                        "                                  declare @OldDataRow varchar(16384);                                                                                                                                   "
                        +
                        "                                  declare @ChannelId varchar(20);"
                        + 
                        "                                  declare @err_notfound EXCEPTION FOR SQLSTATE VALUE '02000';                                                                                                    "
                        +
                        "                                  $(declareOldKeyVariables)                                                                                                                                            "
                        +
                        "                                  $(declareNewKeyVariables)                                                                                                                                            "
                        +
                        "                                  $(custom_before_update_text) " +
                        "                                  if ($(syncOnIncomingBatchCondition)) then begin                                                                                                                           "
                        +
                        "                                    declare DataCursor cursor for                                                                                                                                      "
                        +
                        "                                    $(if:containsBlobClobColumns)                                                                                                                                      "
                        +
                        "                                       select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition);"
                        +
                        "                                    $(else:containsBlobClobColumns)                                                                                                                                    "
                        +
                        "                                       select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames), $(channelExpression) from inserted inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition);                                    "
                        +
                        "                                    $(end:containsBlobClobColumns)                                                                                                                                     "
                        +
                        "                                       open DataCursor;                                                                                                                                                 "
                        +
                        "                                       LoopGetRow:"
                        + 
                        "                                       loop                                                                                                          "
                        +
                        "                                           fetch next DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables), @ChannelId;                                                      "
                        +
                        "                                           if SQLSTATE = @err_notfound then"
                        + 
                        "                                               leave LoopGetRow"
                        + 
                        "                                           end if;                                                                                                                                  "
                        +
                        "                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) "
                        +
                        "                                           values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, @ChannelId, $(txIdExpression), $(defaultCatalog)$(defaultSchema)$(prefixName)_node_disabled(0), $(externalSelect), getdate());"
                        +
                        "                                         end loop LoopGetRow;                                                   "
                        +
                        "                                       close DataCursor;                                                                                                                                                "
                        +
                        "                                    end;                                                                                                                                                                "
                        +
                        "                                  $(custom_on_update_text)                                                                                                                                             "
                        +
                        "                                  end if;"
                        + 
                                                    "end;                                                                                                                                                                 ");
        sqlTemplates.put("deleteTriggerTemplate",
                "create or replace trigger $(triggerName) after delete order 10000 on $(schemaName)$(tableName) referencing old as deleted                                                                                                                              "
                        +
                        "                                begin                                                                                                                                                                  "
                        +
                        "                                  declare @OldPk varchar(2000);                                                                                                                                       "
                        +
                        "                                  declare @OldDataRow varchar(16384);                                                                                                                                  "
                        +
                        "                                  declare @ChannelId varchar(20);                                                                                                                                     "
                        + 
                        "                                  declare @err_notfound EXCEPTION FOR SQLSTATE VALUE '02000';                                                                                                    "
                        +
                        "                                  $(declareOldKeyVariables)                                                                                                                                            "
                        +
                        "                                  $(custom_before_delete_text) " +
                        "                                  if ($(syncOnIncomingBatchCondition)) then begin                                                                                                                           "
                        +
                        "                                    declare DataCursor cursor for                                                                                                                                      "
                        +
                        "                                      select $(oldKeys), $(oldColumns) $(oldKeyNames), $(channelExpression) from deleted where $(syncOnDeleteCondition);                                                                      "
                        +
                        "                                      open DataCursor;                                                                                                                                                  "
                        +
                        "                                      LoopGetRow:"
                        + 
                        "                                      loop                                                                                  "
                        +
                        "                                       fetch next DataCursor into @OldPk,@OldDataRow $(oldKeyVariables), @ChannelId;                                                                                                                                "
                        +
                        "                                           if SQLSTATE = @err_notfound then"
                        + 
                        "                                               leave LoopGetRow"
                        + 
                        "                                           end if;                                                                                                                                  "
                        +
                        "                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) "
                        +
                        "                                           values('$(targetTableName)','D', $(triggerHistoryId), @OldPk, @OldDataRow, @ChannelId, $(txIdExpression), $(defaultCatalog)$(defaultSchema)$(prefixName)_node_disabled(0), $(externalSelect), getdate());"
                        +
                        "                                         end loop LoopGetRow;                                                                                  "
                        +
                        "                                       close DataCursor                                                                                                                                                "
                        +
                        "                                       end;                                                                                                                                           "
                        +
                        "                                  $(custom_on_delete_text)                                                                                                                                             "
                        +
                        "                                end if;"
                        + 
                        "                           end;                                                                                                                                                                    ");
        sqlTemplates.put("initialLoadSqlTemplate",
                "select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                ");
    }

}
