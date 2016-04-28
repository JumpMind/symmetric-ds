/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU General Public License, version 3.0 (GPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU General Public License,
 * version 3.0 (GPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.db.ase;

import java.sql.Types;
import java.util.HashMap;

import org.apache.commons.lang.NotImplementedException;
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

        String quote = symmetricDialect.getPlatform()
                .getDatabaseInfo().getDelimiterToken();
        quote = quote == null ? "\"" : quote;

        emptyColumnTemplate = "null" ;
        stringColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then null else '\"' + str_replace(str_replace($(tableAlias)." + quote + "$(columnName)" + quote + ",'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then null else ('\"' + convert(varchar,$(tableAlias)." + quote + "$(columnName)" + quote + ") + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then null else ('\"' + str_replace(convert(varchar,$(tableAlias)." + quote + "$(columnName)" + quote + ",102),'.','-') + ' ' + right('00'+convert(varchar,datepart(HOUR,$(tableAlias)." + quote + "$(columnName)" + quote + ")),2)+':'+right('00'+convert(varchar,datepart(MINUTE,$(tableAlias)." + quote + "$(columnName)" + quote + ")),2)+':'+right('00'+convert(varchar,datepart(SECOND,$(tableAlias)." + quote + "$(columnName)" + quote + ")),2)+'.'+right('000'+convert(varchar,datepart(MILLISECOND,$(tableAlias)." + quote + "$(columnName)" + quote + ")),3) + '\"') end" ;
        clobColumnTemplate = "case when datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ") is null or datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ")=0 then null else '\"' + str_replace(str_replace(cast($(origTableAlias)." + quote + "$(columnName)" + quote + " as varchar(16384)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias)." + quote + "$(columnName)" + quote + " is null then null else '\"' + bintostr(convert(varbinary(16384),$(origTableAlias)." + quote + "$(columnName)" + quote + ")) + '\"' end" ;
        binaryColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then null else '\"' + bintostr(convert(varbinary(16384),$(tableAlias)." + quote + "$(columnName)" + quote + ")) + '\"' end" ;
        imageColumnTemplate = "case when datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ") is null or datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ")=0 then null else '\"' + bintostr(convert(varbinary(16384),$(origTableAlias)." + quote + "$(columnName)" + quote + ")) + '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then null when $(tableAlias)." + quote + "$(columnName)" + quote + " = 1 then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "+" ;
        newTriggerValue = "inserted" ;
        oldTriggerValue = "deleted" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for insert as                                                                                                                               \n" +
"                                begin                                                                                                                                                                  \n" +
"                                  set nocount on      \n" +
"                                  declare @clientapplname varchar(50)  \n" +
"                                  select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   \n" +
"                                  declare @txid varchar(50)             \n" +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         \n" +
"                                      select @txid = $(txIdExpression)                              \n" +
"                                  end                                                                                                                                                                \n" +
"                                  declare @clientname varchar(50)    \n" +
"                                  select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     \n" +
"                                  declare @DataRow varchar(16384)  \n" + 
"                                  declare @ChannelId varchar(128)   \n" +                                                                                             
"                                  $(declareNewKeyVariables)                                                                                                                                            \n" +
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"                                    declare DataCursor cursor for                                                                                                                                      \n" +
"                                    $(if:containsBlobClobColumns)                                                                                                                                      \n" +
"                                       select $(columns) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)\n" +
"                                    $(else:containsBlobClobColumns)                                                                                                                                    \n" +
"                                       select $(columns) $(newKeyNames), $(channelExpression) from inserted where $(syncOnInsertCondition)                                                                                  \n" +
"                                    $(end:containsBlobClobColumns)                                                                                                                                     \n" +
"                                       open DataCursor                                                                                                                                                 \n" +
"                                       fetch DataCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                                     \n" +
"                                       while @@sqlstatus = 0 begin                                                                                                                                  \n" +
"                                           insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                                             values('$(targetTableName)','I', $(triggerHistoryId), @DataRow, @ChannelId, @txid, @clientname, $(externalSelect), getdate())                                   \n" +
"                                           fetch DataCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                                 \n" +
"                                       end                                                                                                                                                             \n" +
"                                       close DataCursor                                                                                                                                                \n" +
"                                       deallocate cursor DataCursor                                                                                                                                           \n" +
"                                  end                                                                                                                                                                  \n" +
"                                  $(custom_on_insert_text) \n" +
"                                  set nocount off      \n" +
"                                end                                                                                                                                                                    " );




        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for update as                                                                                                                               \n" +
"                                begin                                                                                                                                                                  \n" +
"                                  set nocount on      \n" +
"                                  declare @DataRow varchar(16384)                                                                                                                                      \n" +
"                                  declare @OldPk varchar(2000)                                                                                                                                         \n" +
"                                  declare @OldDataRow varchar(16384)                                                                                                                                   \n" +
"                                  declare @clientapplname varchar(50)  \n" +
"                                  declare @ChannelId varchar(128)   \n" +
"                                  select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   \n" +
"                                  declare @txid varchar(50)                                                                                                                                            \n" +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         \n" +
"                                      select @txid = $(txIdExpression)                             \n" +
"                                  end                                                                                                                                                                \n" +
"                                  declare @clientname varchar(50)    \n" +
"                                  select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     \n" +
"                                  $(declareOldKeyVariables)                                                                                                                                            \n" +
"                                  $(declareNewKeyVariables)                                                                                                                                            \n" +
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"                                    declare DataCursor cursor for                                                                                                                                      \n" +
"                                    $(if:containsBlobClobColumns)                                                                                                                                      \n" +
"                                       select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)\n" +
"                                    $(else:containsBlobClobColumns)                                                                                                                                    \n" +
"                                       select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames), $(channelExpression) from inserted inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)                                    \n" +
"                                    $(end:containsBlobClobColumns)                                                                                                                                     \n" +
"                                       open DataCursor                                                                                                                                                 \n" +
"                                       fetch DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables), @ChannelId                                                             \n" +
"                                       while @@sqlstatus = 0 begin                                                                                                                                  \n" +
"                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                                           values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, @ChannelId, @txid, @clientname, $(externalSelect), getdate())\n" +
"                                         fetch DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables), @ChannelId                                                           \n" +
"                                       end                                                                                                                                                             \n" +
"                                       close DataCursor                                                                                                                                                \n" +
"                                       deallocate cursor DataCursor                                                                                                                                           \n" +
"                                    end                                                                                                                                                                \n" +
"                                  $(custom_on_update_text) \n" +
"                                  set nocount off      \n" +
"                                  end                                                                                                                                                                  " );

        sqlTemplates.put("updateHandleKeyUpdatesTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for update as                                                                                                                             \n" +
"   begin                                                                                                                                                                  \n" +
"     declare @NCT int \n" +
"     set @NCT = @@OPTIONS & 512 \n" +
"     set nocount on                                                                                                                                                       \n" +
"     declare @TransactionId varchar(1000)                                                                                                                                 \n" +
"     declare @DataRow varchar(16384)                                                                                                                                      \n" +
"     declare @OldPk varchar(2000)                                                                                                                                         \n" +
"     declare @OldDataRow varchar(16384)                                                                                                                                     \n" +                                                                                                                                       
"     declare @clientapplname varchar(50)  \n" +
"     select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   \n" +
"     declare @clientname varchar(50)    \n" +
"     select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     \n" +
"     declare @ChannelId varchar(128)                                                                                                                                       \n" +
"     $(declareOldKeyVariables)                                                                                                                                            \n" +
"     $(declareNewKeyVariables)                                                                                                                                            \n" +
"                                  declare @txid varchar(50)                                                                                                                                            \n" +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         \n" +
"                                      select @txid = $(txIdExpression)                             \n" +
"                                  end                                                                                                                                                                \n" +
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"       declare DeleteCursor cursor  for                                                                                                                                \n" +
"          select $(oldKeys), $(oldColumns) $(oldKeyNames) from deleted where $(syncOnDeleteCondition)                                                                      \n" +
"       declare InsertCursor cursor for                                                                                                                                \n" +
"          $(if:containsBlobClobColumns)                                                                                                                                      \n" +
"             select $(columns) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)\n" +
"          $(else:containsBlobClobColumns)                                                                                                                                    \n" +
"             select $(columns) $(newKeyNames), $(channelExpression) from inserted where $(syncOnInsertCondition)                                                                                   \n" +
"          $(end:containsBlobClobColumns)                                                                                                                                     \n" +
"          open DeleteCursor                                                                                                                                                 \n" +
"          open InsertCursor                                                                                                                                                 \n" +
"          fetch next from DeleteCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                          \n" +
"          fetch next from InsertCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                                    \n" +
"          while @@sqlstatus = 0 begin                                                                                                                                  \n" +
"            if ($(dataHasChangedCondition)) begin                                                                                                                                \n" +
"              insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, @ChannelId, @txid, @clientname, $(externalSelect), getdate())\n" +
"            end                                                                                                                                                             \n" +
"            fetch next from DeleteCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                      \n" +
"            fetch next from InsertCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                              \n" +
"          end                                                                                                                                                             \n" +
"          close DeleteCursor                                                                                                                                                \n" +
"          close InsertCursor                                                                                                                                                \n" +
"          deallocate cursor DeleteCursor                                                                                                                                           \n" +
"          deallocate cursor InsertCursor                                                                                                                                           \n" +
"       end                                                                                                                                                                \n" +
"       $(custom_on_update_text)                                                                                                                                             \n" +
"     set nocount off                                                                                                                                        \n" +
"   end                                                                                                                                                                    \n"
);


        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) for delete as                                                                                                                               \n" +
"                                begin                                                                                                                                                                  \n" +
"                                  set nocount on      \n" +
"                                  declare @OldPk varchar(2000)                                                                                                                                         \n" +
"                                  declare @OldDataRow varchar(16384)                                                                                                                                   \n" +
"                                  declare @clientapplname varchar(50)  \n" +
"                                  declare @ChannelId varchar(128)   \n" +
"                                  select @clientapplname = clientapplname from master.dbo.sysprocesses where spid = @@spid   \n" +
"                                  declare @txid varchar(50)                                                                                                                                            \n" +
"                                  if (@@TRANCOUNT > 0) begin                                                                                                                                         \n" +
"                                      select @txid = $(txIdExpression)                            \n" +
"                                  end                                                                                                                                                                \n" +
"                                  declare @clientname varchar(50)    \n" +
"                                  select @clientname = clientname from master.dbo.sysprocesses where spid = @@spid and clientapplname = 'SymmetricDS'     \n" +
"                                  $(declareOldKeyVariables)                                                                                                                                            \n" +
"                                  if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"                                    declare DataCursor cursor for                                                                                                                                      \n" +
"                                      select $(oldKeys), $(oldColumns) $(oldKeyNames), $(channelExpression) from deleted where $(syncOnDeleteCondition)                                                                      \n" +
"                                      open DataCursor                                                                                                                                                  \n" +
"                                       fetch DataCursor into @OldPk, @OldDataRow $(oldKeyVariables), @ChannelId                                                                                          \n" +
"                                       while @@sqlstatus = 0 begin                                                                                                                                  \n" +
"                                         insert into $(defaultCatalog)$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                                           values('$(targetTableName)','D', $(triggerHistoryId), @OldPk, @OldDataRow, @ChannelId, @txid, @clientname, $(externalSelect), getdate())\n" +
"                                         fetch DataCursor into @OldPk,@OldDataRow $(oldKeyVariables), @ChannelId                                                                                         \n" +
"                                       end                                                                                                                                                             \n" +
"                                       close DataCursor                                                                                                                                                \n" +
"                                       deallocate cursor DataCursor                                                                                                                                           \n" +
"                                  end                                                                                                                                                                  \n" +
"                                  $(custom_on_delete_text) \n" +
"                                  set nocount off          \n" +
"                                end                                                                                                                                                                    " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

    @Override
    protected String replaceTemplateVariables(DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table originalTable, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {
        ddl = FormatUtils.replace("oldColumns", trigger.isUseCaptureOldData() ?
                super.buildColumnsString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix, table.getColumns(), dml, true, channel, trigger).toString() : "convert(VARCHAR,null)", ddl);
        ddl = super.replaceTemplateVariables(dml, trigger, history, channel, tablePrefix, originalTable, table,
                defaultCatalog, defaultSchema, ddl);
        Column[] columns = table.getPrimaryKeyColumns();
        ddl = FormatUtils.replace("declareOldKeyVariables",
                buildKeyVariablesDeclare(columns, "old"), ddl);
        ddl = FormatUtils.replace("declareNewKeyVariables",
                buildKeyVariablesDeclare(columns, "new"), ddl);
        return ddl;
    }

    @Override
    protected String buildKeyVariablesDeclare(Column[] columns, String prefix) {
        String text = "";
        for (int i = 0; i < columns.length; i++) {
            text += "declare @" + prefix + "pk" + i + " ";
            switch (columns[i].getMappedTypeCode()) {
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                    // ASE does not support bigint
                    text += "NUMERIC(18,0)\n";
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    // Use same default scale and precision used by Sybase ASA
                    // for a decimal with unspecified scale and precision.
                    text += "decimal(30,6)\n";
                    break;
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                    text += "float\n";
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    text += "varchar(1000)\n";
                    break;
                case Types.DATE:
                    text += "date\n";
                    break;
                case Types.TIME:
                    text += "time\n";
                    break;
                case Types.TIMESTAMP:
                    text += "datetime\n";
                    break;
                case Types.BOOLEAN:
                case Types.BIT:
                    text += "bit\n";
                    break;
                case Types.CLOB:
                    text += "varchar(32767)\n";
                    break;
                case Types.BLOB:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case -10: // SQL-Server ntext binary type
                    text += "varbinary(32767)\n";
                    break;
                case Types.OTHER:
                    text += "varbinary(32767)\n";
                    break;
                default:
                    if (columns[i].getJdbcTypeName() != null
                            && columns[i].getJdbcTypeName().equalsIgnoreCase("interval")) {
                        text += "interval";
                        break;
                    }
                    throw new NotImplementedException(columns[i] + " is of type "
                            + columns[i].getMappedType());
            }
        }

        return text;
    }

}