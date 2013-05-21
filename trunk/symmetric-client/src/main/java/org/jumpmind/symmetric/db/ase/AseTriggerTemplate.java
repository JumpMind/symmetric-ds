package org.jumpmind.symmetric.db.ase;

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

public class AseTriggerTemplate extends AbstractTriggerTemplate {

    public AseTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + str_replace(str_replace($(tableAlias).\"$(columnName)\",'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar,$(tableAlias).\"$(columnName)\") + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + str_replace(convert(varchar,$(tableAlias).\"$(columnName)\",102),'.','-') + ' ' + convert(varchar,$(tableAlias).\"$(columnName)\",108) + '\"') end" ;
        clobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + str_replace(str_replace(cast($(origTableAlias).\"$(columnName)\" as varchar(16384)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + bintostr(convert(varbinary(16384),$(origTableAlias).\"$(columnName)\")) + '\"' end" ;
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
"                                  set nocount on      " +
"                                  declare @clientapplname varchar(50)  " +
"                                  select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   " +
"                                  declare @txid varchar(50)             " +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         " +
"                                      select @txid = convert(varchar, starttime, 20) + '.' + convert(varchar, loid) from master.dbo.systransactions where spid = @@spid                              " +
"                                  end                                                                                                                                                                " +
"                                  declare @clientname varchar(50)    " +
"                                  select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     " +
"                                  declare @DataRow varchar(16384)                                                                                                                                      " +
"                                  $(declareNewKeyVariables)                                                                                                                                            " +
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " +
"                                    declare DataCursor cursor for                                                                                                                                      " +
"                                    $(if:containsBlobClobColumns)                                                                                                                                      " +
"                                       select $(columns) $(newKeyNames) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)" +
"                                    $(else:containsBlobClobColumns)                                                                                                                                    " +
"                                       select $(columns) $(newKeyNames) from inserted where $(syncOnInsertCondition)                                                                                  " +
"                                    $(end:containsBlobClobColumns)                                                                                                                                     " +
"                                       open DataCursor                                                                                                                                                 " +
"                                       fetch DataCursor into @DataRow $(newKeyVariables)                                                                                                     " +
"                                       while @@sqlstatus = 0 begin                                                                                                                                  " +
"                                           insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time) " +
"                                             values('$(targetTableName)','I', $(triggerHistoryId), @DataRow, '$(channelName)', @txid, @clientname, $(externalSelect), getdate())                                   " +
"                                           fetch DataCursor into @DataRow $(newKeyVariables)                                                                                                 " +
"                                       end                                                                                                                                                             " +
"                                       close DataCursor                                                                                                                                                " +
"                                       deallocate cursor DataCursor                                                                                                                                           " +
"                                  end                                                                                                                                                                  " +
"                                  set nocount off      " +
"                                end                                                                                                                                                                    " );




        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for update as                                                                                                                               " +
"                                begin                                                                                                                                                                  " +
"                                  set nocount on      " +
"                                  declare @DataRow varchar(16384)                                                                                                                                      " +
"                                  declare @OldPk varchar(2000)                                                                                                                                         " +
"                                  declare @OldDataRow varchar(16384)                                                                                                                                   " +
"                                  declare @clientapplname varchar(50)  " +
"                                  select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   " +
"                                  declare @txid varchar(50)                                                                                                                                            " +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         " +
"                                      select @txid = convert(varchar, starttime, 20) + '.' + convert(varchar, loid) from master.dbo.systransactions where spid = @@spid                              " +
"                                  end                                                                                                                                                                " +
"                                  declare @clientname varchar(50)    " +
"                                  select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     " +
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
"                                       fetch DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables)                                                             " +
"                                       while @@sqlstatus = 0 begin                                                                                                                                  " +
"                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " +
"                                           values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, '$(channelName)', @txid, @clientname, $(externalSelect), getdate())" +
"                                         fetch DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables)                                                           " +
"                                       end                                                                                                                                                             " +
"                                       close DataCursor                                                                                                                                                " +
"                                       deallocate cursor DataCursor                                                                                                                                           " +
"                                    end                                                                                                                                                                " +
"                                  set nocount off      " +
"                                  end                                                                                                                                                                  " );


        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for delete as                                                                                                                               " +
"                                begin                                                                                                                                                                  " +
"                                  set nocount on      " +
"                                  declare @OldPk varchar(2000)                                                                                                                                         " +
"                                  declare @OldDataRow varchar(16384)                                                                                                                                   " +
"                                  declare @clientapplname varchar(50)  " +
"                                  select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   " +
"                                  declare @txid varchar(50)                                                                                                                                            " +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         " +
"                                      select @txid = convert(varchar, starttime, 20) + '.' + convert(varchar, loid) from master.dbo.systransactions where spid = @@spid                              " +
"                                  end                                                                                                                                                                " +
"                                  declare @clientname varchar(50)    " +
"                                  select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     " +
"                                  $(declareOldKeyVariables)                                                                                                                                            " +
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           " +
"                                    declare DataCursor cursor for                                                                                                                                      " +
"                                      select $(oldKeys), $(oldColumns) $(oldKeyNames) from deleted where $(syncOnDeleteCondition)                                                                      " +
"                                      open DataCursor                                                                                                                                                  " +
"                                       fetch DataCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                          " +
"                                       while @@sqlstatus = 0 begin                                                                                                                                  " +
"                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) " +
"                                           values('$(targetTableName)','D', $(triggerHistoryId), @OldPk, @OldDataRow, '$(channelName)', @txid, @clientname, $(externalSelect), getdate())" +
"                                         fetch DataCursor into @OldPk,@OldDataRow $(oldKeyVariables)                                                                                         " +
"                                       end                                                                                                                                                             " +
"                                       close DataCursor                                                                                                                                                " +
"                                       deallocate cursor DataCursor                                                                                                                                           " +
"                                  end                                                                                                                                                                  " +
"                                  set nocount off      " +
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
