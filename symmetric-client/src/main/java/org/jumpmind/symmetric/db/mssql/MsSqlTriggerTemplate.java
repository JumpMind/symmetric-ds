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
package org.jumpmind.symmetric.db.mssql;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.util.HashMap;

import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.util.FormatUtils;

public class MsSqlTriggerTemplate extends AbstractTriggerTemplate {

    boolean castToNVARCHAR;

    public MsSqlTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);

        castToNVARCHAR = symmetricDialect.getParameterService().is(ParameterConstants.MSSQL_USE_NTYPES_FOR_SYNC);
        
        String triggerExecuteAs = symmetricDialect.getParameterService().getString(ParameterConstants.MSSQL_TRIGGER_EXECUTE_AS, "self");
        
        String defaultCatalog = symmetricDialect.getParameterService().is(ParameterConstants.MSSQL_INCLUDE_CATALOG_IN_TRIGGERS, true) ? "$(defaultCatalog)" : "";
        
        // @formatter:off
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert("+
        (castToNVARCHAR ? "n" : "")
        +"varchar($(columnSize)),$(tableAlias).\"$(columnName)\") $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        geometryColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert("+(castToNVARCHAR ? "n" : "")+"varchar(max),$(tableAlias).\"$(columnName)\".STAsText()) $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        geographyColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert("+(castToNVARCHAR ? "n" : "")+"varchar(max),$(tableAlias).\"$(columnName)\".STAsText()) $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar(40), $(tableAlias).\"$(columnName)\",2) + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar,$(tableAlias).\"$(columnName)\",121) + '\"') end" ;
        clobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(cast($(origTableAlias).\"$(columnName)\" as "+(castToNVARCHAR ? "n" : "")+"varchar(max)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(" + defaultCatalog + "dbo.$(prefixName)_base64_encode(CONVERT(VARBINARY(max), $(origTableAlias).\"$(columnName)\")),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        binaryColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(" + defaultCatalog + "dbo.$(prefixName)_base64_encode(CONVERT(VARBINARY(max), $(tableAlias).\"$(columnName)\")),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' when $(tableAlias).\"$(columnName)\" = 1 then '\"1\"' else '\"0\"' end" ;
        triggerConcatCharacter = "+" ;
        newTriggerValue = "inserted" ;
        oldTriggerValue = "deleted" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;

        sqlTemplates = new HashMap<String,String>();

        sqlTemplates.put("insertTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) with execute as "+triggerExecuteAs+" after insert as                                                                                                \n" +
"   begin                                                                                                                                                                  \n" +
"     declare @NCT int \n" +
"     set @NCT = @@OPTIONS & 512 \n" +
"     set nocount on                                                                                                                                                       \n" +
"     declare @TransactionId varchar(1000)                                                                                                                                 \n" +
"     declare @DataRow "+(castToNVARCHAR ? "n" : "")+"varchar(max)                                                                                                                                        \n" +
"     declare @ChannelId varchar(128)                                                                                                                                       \n" +
"     $(declareNewKeyVariables)                                                                                                                                            \n" +
"     if (@@TRANCOUNT > 0) begin                                                                                                                                           \n" +
"       select @TransactionId = convert(VARCHAR(1000),transaction_id) from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                     \n" +
"     end                                                                                                                                                                  \n" +
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"       declare DataCursor cursor local for                                                                                                                                \n" +
"       $(if:containsBlobClobColumns)                                                                                                                                      \n" +
"          select $(columns) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)\n" +
"       $(else:containsBlobClobColumns)                                                                                                                                    \n" +
"          select $(columns) $(newKeyNames), $(channelExpression) from inserted where $(syncOnInsertCondition)                                                                                   \n" +
"       $(end:containsBlobClobColumns)                                                                                                                                     \n" +
"          open DataCursor                                                                                                                                                 \n" +
"          fetch next from DataCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                                \n" +
"          while @@FETCH_STATUS = 0 begin                                                                                                                                  \n" +
"              insert into " + defaultCatalog + "$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                values('$(targetTableName)','I', $(triggerHistoryId), @DataRow, @ChannelId, $(txIdExpression), " + defaultCatalog + "dbo.$(prefixName)_node_disabled(), $(externalSelect), current_timestamp) \n" +
"              fetch next from DataCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                                 \n" +
"          end                                                                                                                                                             \n" +
"          close DataCursor                                                                                                                                                \n" +
"          deallocate DataCursor                                                                                                                                           \n" +
"     end                                                                                                                                                                  \n" +
"     $(custom_on_insert_text)                                                                                                                                             \n" +
"     if (@NCT = 0) set nocount off                                                                                                                                        \n" +
"   end                                                                                                                                                                    \n" +
"---- go");

        sqlTemplates.put("updateTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) with execute as "+triggerExecuteAs+" after update as                                                                                                \n" +
"   begin                                                                                                                                                                  \n" +
"     declare @NCT int \n" +
"     set @NCT = @@OPTIONS & 512 \n" +
"     set nocount on                                                                                                                                                       \n" +
"     declare @TransactionId varchar(1000)                                                                                                                                 \n" +
"     declare @DataRow "+(castToNVARCHAR ? "n" : "")+"varchar(max)                                                                                                                                        \n" +
"     declare @OldPk "+(castToNVARCHAR ? "n" : "")+"varchar(2000)                                                                                                                                         \n" +
"     declare @OldDataRow "+(castToNVARCHAR ? "n" : "")+"varchar(max)                                                                                                                                     \n" +
"     declare @ChannelId varchar(128)                                                                                                                                       \n" +
"     $(declareOldKeyVariables)                                                                                                                                            \n" +
"     $(declareNewKeyVariables)                                                                                                                                            \n" +
"     if (@@TRANCOUNT > 0) begin                                                                                                                                           \n" +
"       select @TransactionId = convert(VARCHAR(1000),transaction_id) from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                     \n" +
"     end                                                                                                                                                                  \n" +
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"       declare DataCursor cursor local for                                                                                                                                \n" +
"       $(if:containsBlobClobColumns)                                                                                                                                      \n" +
"          select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)\n" +
"       $(else:containsBlobClobColumns)                                                                                                                                    \n" +
"          select $(columns), $(oldKeys), $(oldColumns) $(oldKeyNames) $(newKeyNames), $(channelExpression) from inserted inner join deleted on $(oldNewPrimaryKeyJoin) where $(syncOnUpdateCondition)                                    \n" +
"       $(end:containsBlobClobColumns)                                                                                                                                     \n" +
"          open DataCursor                                                                                                                                                 \n" +
"          fetch next from DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables), @ChannelId                                                              \n" +
"          while @@FETCH_STATUS = 0 begin  \n" +
"            if ($(dataHasChangedCondition)) begin                                                                                                                                \n" +
"              insert into " + defaultCatalog + "$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, @ChannelId, $(txIdExpression), " + defaultCatalog + "dbo.$(prefixName)_node_disabled(), $(externalSelect), current_timestamp)\n" +
"            end \n" +
"            fetch next from DataCursor into @DataRow, @OldPk, @OldDataRow $(oldKeyVariables) $(newKeyVariables), @ChannelId                                                           \n" +
"          end                                                                                                                                                             \n" +
"          close DataCursor                                                                                                                                                \n" +
"          deallocate DataCursor                                                                                                                                           \n" +
"       end                                                                                                                                                                \n" +
"       $(custom_on_update_text)                                                                                                                                             \n" +
"     if (@NCT = 0) set nocount off                                                                                                                                        \n" +
"   end                                                                                                                                                                    \n" +
"---- go");

        sqlTemplates.put("updateHandleKeyUpdatesTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) with execute as "+triggerExecuteAs+" after update as                                                                                                                             \n" +
"   begin                                                                                                                                                                  \n" +
"     declare @NCT int \n" +
"     set @NCT = @@OPTIONS & 512 \n" +
"     set nocount on                                                                                                                                                       \n" +
"     declare @TransactionId varchar(1000)                                                                                                                                 \n" +
"     declare @OldPk "+(castToNVARCHAR ? "n" : "")+"varchar(2000)                                                                                                                                         \n" +
"     declare @OldDataRow "+(castToNVARCHAR ? "n" : "")+"varchar(max)                                                                                                                                     \n" +
"     declare @DataRow "+(castToNVARCHAR ? "n" : "")+"varchar(max)                                                                                                                                        \n" +
"     declare @ChannelId varchar(128)                                                                                                                                       \n" +
"     $(declareOldKeyVariables)                                                                                                                                            \n" +
"     $(declareNewKeyVariables)                                                                                                                                            \n" +
"                                                                                                                                                                          \n" +
"     if (@@TRANCOUNT > 0) begin                                                                                                                                           \n" +
"       select @TransactionId = convert(VARCHAR(1000),transaction_id) from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                                            \n" +
"     end                                                                                                                                                                  \n" +
"     if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"       declare DeleteCursor cursor local for                                                                                                                                \n" +
"          select $(oldKeys), $(oldColumns) $(oldKeyNames) from deleted where $(syncOnDeleteCondition)                                                                      \n" +
"       declare InsertCursor cursor local for                                                                                                                                \n" +
"          $(if:containsBlobClobColumns)                                                                                                                                      \n" +
"             select $(columns) $(newKeyNames), $(channelExpression) from inserted inner join $(schemaName)$(tableName) $(origTableAlias) on $(tableNewPrimaryKeyJoin) where $(syncOnInsertCondition)\n" +
"          $(else:containsBlobClobColumns)                                                                                                                                    \n" +
"             select $(columns) $(newKeyNames), $(channelExpression) from inserted where $(syncOnInsertCondition)                                                                                   \n" +
"          $(end:containsBlobClobColumns)                                                                                                                                     \n" +
"          open DeleteCursor                                                                                                                                                 \n" +
"          open InsertCursor                                                                                                                                                 \n" +
"          fetch next from DeleteCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                          \n" +
"          fetch next from InsertCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                                    \n" +
"          while @@FETCH_STATUS = 0 begin                                                                                                                                  \n" +
"            if ($(dataHasChangedCondition)) begin                                                                                                                                \n" +
"              insert into " + defaultCatalog + "$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, row_data, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"                values('$(targetTableName)','U', $(triggerHistoryId), @DataRow, @OldPk, @OldDataRow, @ChannelId, $(txIdExpression), " + defaultCatalog + "dbo.$(prefixName)_node_disabled(), $(externalSelect), current_timestamp)\n" +
"            end                                                                                                                                                             \n" +
"            fetch next from DeleteCursor into @OldPk, @OldDataRow $(oldKeyVariables)                                                                                      \n" +
"            fetch next from InsertCursor into @DataRow $(newKeyVariables), @ChannelId                                                                                              \n" +
"          end                                                                                                                                                             \n" +
"          close DeleteCursor                                                                                                                                                \n" +
"          close InsertCursor                                                                                                                                                \n" +
"          deallocate DeleteCursor                                                                                                                                           \n" +
"          deallocate InsertCursor                                                                                                                                           \n" +
"       end                                                                                                                                                                \n" +
"       $(custom_on_update_text)                                                                                                                                             \n" +
"     if (@NCT = 0) set nocount off                                                                                                                                        \n" +
"   end                                                                                                                                                                    \n" +
"---- go");

        sqlTemplates.put("deleteTriggerTemplate" ,
"create trigger $(triggerName) on $(schemaName)$(tableName) with execute as "+triggerExecuteAs+" after delete as                                                                                                                             \n" +
"  begin                                                                                                                                                                  \n" +
"    declare @NCT int \n" +
"    set @NCT = @@OPTIONS & 512 \n" +
"    set nocount on                                                                                                                                                       \n" +
"    declare @TransactionId varchar(1000)                                                                                                                                 \n" +
"    declare @OldPk "+(castToNVARCHAR ? "n" : "")+"varchar(2000)                                                                                                                                         \n" +
"    declare @OldDataRow "+(castToNVARCHAR ? "n" : "")+"varchar(max)                                                                                                                                     \n" +
"    declare @ChannelId varchar(128)                                                                                                                                       \n" +
"    $(declareOldKeyVariables)                                                                                                                                            \n" +
"    if (@@TRANCOUNT > 0) begin                                                                                                                                           \n" +
"       select @TransactionId = convert(VARCHAR(1000),transaction_id)    from sys.dm_exec_requests where session_id=@@SPID and open_transaction_count > 0                                           \n" +
"    end                                                                                                                                                                  \n" +
"    if ($(syncOnIncomingBatchCondition)) begin                                                                                                                           \n" +
"      declare DataCursor cursor local for                                                                                                                                \n" +
"        select $(oldKeys), $(oldColumns) $(oldKeyNames), $(channelExpression) from deleted where $(syncOnDeleteCondition)                                                                      \n" +
"        open DataCursor                                                                                                                                                  \n" +
"         fetch next from DataCursor into @OldPk, @OldDataRow $(oldKeyVariables), @ChannelId                                                                                      \n" +
"         while @@FETCH_STATUS = 0 begin                                                                                                                                  \n" +
"           insert into " + defaultCatalog + "$(defaultSchema)$(prefixName)_data (table_name, event_type, trigger_hist_id, pk_data, old_data, channel_id, transaction_id, source_node_id, external_data, create_time) \n" +
"             values('$(targetTableName)','D', $(triggerHistoryId), @OldPk, @OldDataRow, @ChannelId, $(txIdExpression), " + defaultCatalog + "dbo.$(prefixName)_node_disabled(), $(externalSelect), current_timestamp)\n" +
"           fetch next from DataCursor into @OldPk,@OldDataRow $(oldKeyVariables), @ChannelId                                                                                      \n" +
"         end                                                                                                                                                             \n" +
"         close DataCursor                                                                                                                                                \n" +
"         deallocate DataCursor                                                                                                                                           \n" +
"    end                                                                                                                                                                  \n" +
"    $(custom_on_delete_text)                                                                                                                                              \n" +
"    if (@NCT = 0) set nocount off                                                                                                                                         \n" +
"   end                                                                                                                                                                    \n" +
"---- go");

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause) " );

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

    protected String getSourceTablePrefix(TriggerHistory triggerHistory) {
        String prefix = (isNotBlank(triggerHistory.getSourceSchemaName()) ? SymmetricUtils.quote(
                symmetricDialect, triggerHistory.getSourceSchemaName()) + symmetricDialect.getPlatform().getDatabaseInfo().getSchemaSeparator() : "");
        prefix = (isNotBlank(triggerHistory.getSourceCatalogName()) ? SymmetricUtils.quote(
                symmetricDialect, triggerHistory.getSourceCatalogName()) + symmetricDialect.getPlatform().getDatabaseInfo().getCatalogSeparator() : "")
                + prefix;
        if (isBlank(prefix)) {
            prefix = (isNotBlank(symmetricDialect.getPlatform().getDefaultSchema()) ? SymmetricUtils
                    .quote(symmetricDialect, symmetricDialect.getPlatform().getDefaultSchema())
                    + "." : "");
            
            if (symmetricDialect.getParameterService().is(ParameterConstants.MSSQL_INCLUDE_CATALOG_IN_TRIGGERS, true)) {
	            prefix = (isNotBlank(symmetricDialect.getPlatform().getDefaultCatalog()) ? SymmetricUtils
	                    .quote(symmetricDialect, symmetricDialect.getPlatform().getDefaultCatalog())
	                    + "." : "") + prefix;
            }
        }
        return prefix;
    }

    @Override
    protected String getColumnSize(Column column) {
        if (castToNVARCHAR && column.getSizeAsInt() > 4000) {
            return "max";
        }
        return column.getSize();
    }

    @Override
    protected boolean requiresEmptyLobTemplateForDeletes() {
        return true;
    }
}