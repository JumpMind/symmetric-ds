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
package org.jumpmind.symmetric.db.mssql2000;

import java.util.HashMap;

import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.db.mssql.MsSqlTriggerTemplate;

public class MsSql2000TriggerTemplate extends MsSqlTriggerTemplate {

    public MsSql2000TriggerTemplate(ISymmetricDialect symmetricDialect) {
        this.symmetricDialect = symmetricDialect;

        // @formatter:off
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert(varchar(8000),$(tableAlias).\"$(columnName)\") $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        geometryColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(convert(varchar(8000),$(tableAlias).\"$(columnName)\".STAsText()) $(masterCollation),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar, $(tableAlias).\"$(columnName)\",2) + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else ('\"' + convert(varchar,$(tableAlias).\"$(columnName)\",121) + '\"') end" ;
        clobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace(cast($(origTableAlias).\"$(columnName)\" as varchar(8000)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias).\"$(columnName)\" is null then '' else '\"' + replace(replace($(defaultCatalog)dbo.sym_base64_encode(CONVERT(VARBINARY(8000), $(origTableAlias).\"$(columnName)\")),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
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
            "     set nocount on;                                                                                                                                                             " +
            "     declare @TransactionId varchar(1000)                                                                                                                                 " +
            "     declare @DataRow varchar(8000)                                                                                                                                        " +
            "     $(declareNewKeyVariables)                                                                                                                                            " +
            "     if (@@TRANCOUNT > 0) begin                                                                                                                                           " +
            "       execute sp_getbindtoken @TransactionId output; " +
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
            "     $(custom_on_insert_text)                                                                                                                                             " +
            "     set nocount off                                                                                                                                                      " +
            "   end                                                                                                                                                                    " );

        sqlTemplates.put("updateTriggerTemplate" ,
            "create trigger $(triggerName) on $(schemaName)$(tableName) after update as                                                                                                                             " +
            "   begin     " +
            "     set nocount on;                                                                                                                                                             " +
            "     declare @TransactionId varchar(1000)                                                                                                                                 " +
            "     declare @DataRow varchar(8000)                                                                                                                                        " +
            "     declare @OldPk varchar(2000)                                                                                                                                         " +
            "     declare @OldDataRow varchar(8000)                                                                                                                                     " +
            "     $(declareOldKeyVariables)                                                                                                                                            " +
            "     $(declareNewKeyVariables)                                                                                                                                            " +
            "     if (@@TRANCOUNT > 0) begin                                                                                                                                           " +
            "       execute sp_getbindtoken @TransactionId output; " +
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
            "       $(custom_on_update_text)                                                                                                                                             " +
            "       set nocount off                                                                                                                                                      " +
            "     end                                                                                                                                                                  " );

        sqlTemplates.put("updateHandleKeyUpdatesTriggerTemplate" ,
            "create trigger $(triggerName) on $(schemaName)$(tableName) after update as                                                                                                                             " +
            "   begin                                                                                                                                                                  " +
            "     set nocount on;                                                                                                                                                             " +
            "     declare @TransactionId varchar(1000)                                                                                                                                 " +
            "     declare @OldPk varchar(2000)                                                                                                                                         " +
            "     declare @OldDataRow varchar(8000)                                                                                                                                     " +
            "     declare @DataRow varchar(8000)                                                                                                                                        " +
            "     $(declareOldKeyVariables)                                                                                                                                            " +
            "     $(declareNewKeyVariables)                                                                                                                                            " +
            "     if (@@TRANCOUNT > 0) begin                                                                                                                                           " +
            "       execute sp_getbindtoken @TransactionId output; " +
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
            "       $(custom_on_update_text)                                                                                                                                             " +
            "       set nocount off                                                                                                                                                      " +
            "     end                                                                                                                                                                  " );

        sqlTemplates.put("deleteTriggerTemplate" ,
            "create trigger $(triggerName) on $(schemaName)$(tableName) after delete as                                                                                                                             " +
            "  begin                                                                                                                                                                  " +
            "    set nocount on;                                                                                                                                                             " +
            "    declare @TransactionId varchar(1000)                                                                                                                                 " +
            "    declare @OldPk varchar(2000)                                                                                                                                         " +
            "    declare @OldDataRow varchar(8000)                                                                                                                                     " +
            "    $(declareOldKeyVariables)                                                                                                                                            " +
            "    if (@@TRANCOUNT > 0) begin                                                                                                                                           " +
            "       execute sp_getbindtoken @TransactionId output; " +
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
            "     $(custom_on_delete_text)                                                                                                                                             " +
            "     set nocount off                                                                                                                                                      " +
            "  end                                                                                                                                                                    " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
                "select $(columns) from $(schemaName)$(tableName) t where $(whereClause) " );

    }

}
