package org.jumpmind.symmetric.db.mssql;

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

public class MsSqlTriggerTemplate extends AbstractTriggerTemplate {
    
    public MsSqlTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        
        // @formatter:off
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert(varchar(max),$(tableAlias).\"$(columnName)\") $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        geometryColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert(varchar(max),$(tableAlias).\"$(columnName)\".STAsText()) $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;        
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar, $(tableAlias).\"$(columnName)\",2) + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar,$(tableAlias).\"$(columnName)\",121) + '\"') end" ;
        clobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(cast($(origTableAlias).\"$(columnName)\" as varchar(max)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace($(defaultCatalog)dbo.sym_base64_encode(CONVERT(VARBINARY(max), $(origTableAlias).\"$(columnName)\")),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" = 1 then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "+" ;
        newTriggerValue = "inserted" ;
        oldTriggerValue = "deleted" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();

        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) after insert as                                                                                                                             " + 
"   begin                                                                                                                                                                  " + 
"     set nocount on                                                                                                                                                       " + 
"     declare @TransactionId varchar(1000)                                                                                                                                 " + 
"     declare @DataRow varchar(max)                                                                                                                                        " + 
"     $(declareNewKeyVariables)                                                                                                                                            " + 
"     if (@@TRANCOUNT > 0) begin                                                                                                                                           " + 
"       select @TransactionId = convert(VARCHAR(1000),transaction_id) from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                                            " + 
"     end                                                                                                                                                                  " + 
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"       declare DataCursor cursor local for                                                                                                                                " + 
"       $(if:containsBlobClobColumns)                                                                                                                                      " + 
"          select $(columns) $(newKeyNames) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)" + 
"       $(else:containsBlobClobColumns)                                                                                                                                    " + 
"          select $(columns) $(newKeyNames) from inserted where $(syncOnInsertCondition)                                                                                   " + 
"       $(end:containsBlobClobColumns)                                                                                                                                     " + 
"          open DataCursor                                                                                                                                                 " + 
"          fetch next from DataCursor into @DataRow $(newKeyVariables)                                                                                                     " + 
"          while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"              insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"                values('$(targetTableName)','I', $(triggerHistoryId), @DataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(), $(externalSelect), current_timestamp)                                   " + 
"              fetch next from DataCursor into @DataRow $(newKeyVariables)                                                                                                 " + 
"          end                                                                                                                                                             " + 
"          close DataCursor                                                                                                                                                " + 
"          deallocate DataCursor                                                                                                                                           " + 
"     end                                                                                                                                                                  " + 
"     set nocount off                                                                                                                                                      " + 
"   end                                                                                                                                                                    " );

        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) after update as                                                                                                                             " + 
"   begin                                                                                                                                                                  " + 
"     set nocount on                                                                                                                                                       " + 
"     declare @TransactionId varchar(1000)                                                                                                                                 " + 
"     declare @DataRow varchar(max)                                                                                                                                        " + 
"     declare @OldPk varchar(2000)                                                                                                                                         " + 
"     declare @OldDataRow varchar(max)                                                                                                                                     " + 
"     $(declareOldKeyVariables)                                                                                                                                            " + 
"     $(declareNewKeyVariables)                                                                                                                                            " + 
"     if (@@TRANCOUNT > 0) begin                                                                                                                                           " + 
"       select @TransactionId = convert(VARCHAR(1000),transaction_id) from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                                            " + 
"     end                                                                                                                                                                  " + 
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"       declare DataCursor cursor local for                                                                                                                                " + 
"       $(if:containsBlobClobColumns)                                                                                                                                      " + 
"          select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)" + 
"       $(else:containsBlobClobColumns)                                                                                                                                    " + 
"          select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames) from inserted inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)                                    " + 
"       $(end:containsBlobClobColumns)                                                                                                                                     " + 
"          open DataCursor                                                                                                                                                 " + 
"          fetch next from DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables)                                                             " + 
"          while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"            insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"              values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(), $(externalSelect), current_timestamp)" + 
"            fetch next from DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables)                                                           " + 
"          end                                                                                                                                                             " + 
"          close DataCursor                                                                                                                                                " + 
"          deallocate DataCursor                                                                                                                                           " + 
"       end                                                                                                                                                                " + 
"       set nocount off                                                                                                                                                    " + 
"     end                                                                                                                                                                  " );

        sqlTemplates.put("updateHandleKeyUpdatesTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) after update as                                                                                                                             " + 
"   begin                                                                                                                                                                  " + 
"     set nocount on                                                                                                                                                       " + 
"     declare @TransactionId varchar(1000)                                                                                                                                 " +
"     declare @OldPk varchar(2000)                                                                                                                                         " +                                                                                                                                             
"     declare @OldDataRow varchar(max)                                                                                                                                     " + 
"     declare @DataRow varchar(max)                                                                                                                                        " +  
"     $(declareOldKeyVariables)                                                                                                                                            " + 
"     $(declareNewKeyVariables)                                                                                                                                            " + 
"                                                                                                                                                                          " +
"     if (@@TRANCOUNT > 0) begin                                                                                                                                           " + 
"       select @TransactionId = convert(VARCHAR(1000),transaction_id) from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                                            " + 
"     end                                                                                                                                                                  " + 
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"       declare DeleteCursor cursor local for                                                                                                                                " + 
"          select $(oldKeys), $(oldColumns) $(oldKeyNames) from deleted where $(syncOnDeleteCondition)                                                                      " + 
"       declare InsertCursor cursor local for                                                                                                                                " + 
"          $(if:containsBlobClobColumns)                                                                                                                                      " + 
"             select $(columns) $(newKeyNames) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)" + 
"          $(else:containsBlobClobColumns)                                                                                                                                    " + 
"             select $(columns) $(newKeyNames) from inserted where $(syncOnInsertCondition)                                                                                   " + 
"          $(end:containsBlobClobColumns)                                                                                                                                     " + 
"          open DeleteCursor                                                                                                                                                 " + 
"          open InsertCursor                                                                                                                                                 " + 
"          fetch next from DeleteCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                          " + 
"          fetch next from InsertCursor into @DataRow $(newKeyVariables)                                                                                                    " +
"          while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"            insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"              values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(), $(externalSelect), current_timestamp)" + 
"            fetch next from DeleteCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                      " + 
"            fetch next from InsertCursor into @DataRow $(newKeyVariables)                                                                                                 " + 
"          end                                                                                                                                                             " + 
"          close DeleteCursor                                                                                                                                                " + 
"          close InsertCursor                                                                                                                                                " + 
"          deallocate DeleteCursor                                                                                                                                           " + 
"          deallocate InsertCursor                                                                                                                                           " + 
"       end                                                                                                                                                                " + 
"       set nocount off                                                                                                                                                    " + 
"     end                                                                                                                                                                  " );        
        
        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) after delete as                                                                                                                             " + 
"  begin                                                                                                                                                                  " + 
"    set nocount on                                                                                                                                                       " + 
"    declare @TransactionId varchar(1000)                                                                                                                                 " + 
"    declare @OldPk varchar(2000)                                                                                                                                         " + 
"    declare @OldDataRow varchar(max)                                                                                                                                     " + 
"    $(declareOldKeyVariables)                                                                                                                                            " + 
"    if (@@TRANCOUNT > 0) begin                                                                                                                                           " + 
"       select @TransactionId = convert(VARCHAR(1000),transaction_id)    from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                                           " + 
"    end                                                                                                                                                                  " + 
"    if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " + 
"      declare DataCursor cursor local for                                                                                                                                " + 
"        select $(oldKeys), $(oldColumns) $(oldKeyNames) from deleted where $(syncOnDeleteCondition)                                                                      " + 
"        open DataCursor                                                                                                                                                  " + 
"         fetch next from DataCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                          " + 
"         while @@FETCH_STATUS = 0 begin                                                                                                                                  " + 
"           insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " + 
"             values('$(targetTableName)','D', $(triggerHistoryId), @OldPk, @OldDataRow, '$(channelName)', $(txIdExpression), $(defaultCatalog)dbo.sym_node_disabled(), $(externalSelect), current_timestamp)" + 
"           fetch next from DataCursor into @OldPk,@OldDataRow $(oldKeyVariables)                                                                                         " + 
"         end                                                                                                                                                             " + 
"         close DataCursor                                                                                                                                                " + 
"         deallocate DataCursor                                                                                                                                           " + 
"    end                                                                                                                                                                  " + 
"    set nocount off                                                                                                                                                      " + 
"  end                                                                                                                                                                    " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause) " );

    }

    @Override
    public String replaceTemplateVariables(DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {
        ddl =  super.replaceTemplateVariables(dml, trigger, history, channel, tablePrefix, table,
                defaultCatalog, defaultSchema, ddl);
        Column[] columns = table.getPrimaryKeyColumns();
        ddl = FormatUtils.replace("declareOldKeyVariables",
                buildKeyVariablesDeclare(columns, "old"), ddl);
        ddl = FormatUtils.replace("declareNewKeyVariables",
                buildKeyVariablesDeclare(columns, "new"), ddl);
        return ddl;
    }
    
    @Override
    protected boolean requiresEmptyLobTemplateForDeletes() {
        return true;
    }
}