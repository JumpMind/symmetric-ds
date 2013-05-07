package org.jumpmind.symmetric.db.sybase;

import java.util.HashMap;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.util.FormatUtils;

public class SybaseTriggerTemplate extends AbstractTriggerTemplate {

    public SybaseTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect); 
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + str_replace(str_replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + cast($(tableAlias).\"$(columnName)\" as varchar) + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + str_replace(convert(varchar,$(tableAlias).\"$(columnName)\",23),'T',' ') + '\"') end" ;
        clobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + str_replace(str_replace(cast($(origTableAlias).\"$(columnName)\" as varchar(16384)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + str_replace(str_replace(dbo.sym_base64_encode($(origTableAlias).\"$(columnName)\"),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" = 1 then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "+" ;
        newTriggerValue = "inserted" ;
        oldTriggerValue = "deleted" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for insert as                                                                                                                               " + 
"                                begin                                                                                                                                                                  " + 
"                                  declare @DataRow varchar(16384)                                                                                                                                      " + 
"                                  $(declareNewKeyVariables)                                                                                                                                            " + 
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"                                    declare DataCursor cursor for                                                                                                                                      " + 
"                                    $(if:containsBlobClobColumns)                                                                                                                                      " + 
"                                       select $(columns), $(newKeyNames) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)" + 
"                                    $(else:containsBlobClobColumns)                                                                                                                                    " + 
"                                       select $(columns), $(newKeyNames) from inserted where $(syncOnInsertCondition)                                                                                  " + 
"                                    $(end:containsBlobClobColumns)                                                                                                                                     " + 
"                                       open DataCursor                                                                                                                                                 " + 
"                                       fetch next from DataCursor into @DataRow $(newKeyVariables)                                                                                                     " + 
"                                       while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"                                           insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"                                             values('$(targetTableName)','I', $(triggerHistoryId), @DataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(0), $(externalSelect), getdate())                                   " + 
"                                           fetch next from DataCursor into @DataRow $(newKeyVariables)                                                                                                 " + 
"                                       end                                                                                                                                                             " + 
"                                       close DataCursor                                                                                                                                                " + 
"                                       deallocate DataCursor                                                                                                                                           " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for update as                                                                                                                               " + 
"                                begin                                                                                                                                                                  " + 
"                                  declare @DataRow varchar(16384)                                                                                                                                      " + 
"                                  declare @OldPk varchar(2000)                                                                                                                                         " + 
"                                  declare @OldDataRow varchar(16384)                                                                                                                                   " + 
"                                  $(declareOldKeyVariables)                                                                                                                                            " + 
"                                  $(declareNewKeyVariables)                                                                                                                                            " + 
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"                                    declare DataCursor cursor for                                                                                                                                      " + 
"                                    $(if:containsBlobClobColumns)                                                                                                                                      " + 
"                                       select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)" + 
"                                    $(else:containsBlobClobColumns)                                                                                                                                    " + 
"                                       select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames) from inserted inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)                                    " + 
"                                    $(end:containsBlobClobColumns)                                                                                                                                     " + 
"                                       open DataCursor                                                                                                                                                 " + 
"                                       fetch next from DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables)                                                             " + 
"                                       while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"                                           values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(0), $(externalSelect), getdate())" + 
"                                         fetch next from DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables)                                                           " + 
"                                       end                                                                                                                                                             " + 
"                                       close DataCursor                                                                                                                                                " + 
"                                       deallocate DataCursor                                                                                                                                           " + 
"                                    end                                                                                                                                                                " + 
"                                  end                                                                                                                                                                  " );
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for delete as                                                                                                                               " + 
"                                begin                                                                                                                                                                  " + 
"                                  declare @OldPk varchar(2000)                                                                                                                                         " + 
"                                  declare @OldDataRow varchar(16384)                                                                                                                                   " + 
"                                  $(declareOldKeyVariables)                                                                                                                                            " + 
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"                                    declare DataCursor cursor for                                                                                                                                      " + 
"                                      select $(oldKeys), $(oldColumns) $(oldKeyNames) from deleted where $(syncOnDeleteCondition)                                                                      " + 
"                                      open DataCursor                                                                                                                                                  " + 
"                                       fetch next from DataCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                          " + 
"                                       while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"                                           values('$(targetTableName)','D', $(triggerHistoryId), @OldPk, @OldDataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(0), $(externalSelect), getdate())" + 
"                                         fetch next from DataCursor into @OldPk,@OldDataRow $(oldKeyVariables)                                                                                         " + 
"                                       end                                                                                                                                                             " + 
"                                       close DataCursor                                                                                                                                                " + 
"                                       deallocate DataCursor                                                                                                                                           " + 
"                                  end                                                                                                                                                                  " + 
"                                end                                                                                                                                                                    " );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }
    
    @Override
    protected String replaceTemplateVariables(DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table originalTable, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {
        ddl =  super.replaceTemplateVariables(dml, trigger, history, channel, tablePrefix, originalTable, table,
                defaultCatalog, defaultSchema, ddl);
        Column[] columns = table.getPrimaryKeyColumns();
        ddl = FormatUtils.replace("declareOldKeyVariables",
                buildKeyVariablesDeclare(columns, "old"), ddl);
        ddl = FormatUtils.replace("declareNewKeyVariables",
                buildKeyVariablesDeclare(columns, "new"), ddl);
        return ddl;
    }

}