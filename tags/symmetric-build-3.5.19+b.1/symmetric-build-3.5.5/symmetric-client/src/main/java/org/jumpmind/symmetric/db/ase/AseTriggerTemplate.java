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

        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then '' else '\"' + str_replace(str_replace($(tableAlias)." + quote + "$(columnName)" + quote + ",'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        numberColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then '' else ('\"' + convert(varchar,$(tableAlias)." + quote + "$(columnName)" + quote + ") + '\"') end" ;
        datetimeColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then '' else ('\"' + str_replace(convert(varchar,$(tableAlias)." + quote + "$(columnName)" + quote + ",102),'.','-') + ' ' + convert(varchar,$(tableAlias)." + quote + "$(columnName)" + quote + ",108) + '\"') end" ;
        clobColumnTemplate = "case when datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ") is null or datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ")=0 then '' else '\"' + str_replace(str_replace(cast($(origTableAlias)." + quote + "$(columnName)" + quote + " as varchar(16384)),'\\','\\\\'),'\"','\\\"') + '\"' end" ;
        blobColumnTemplate = "case when $(origTableAlias)." + quote + "$(columnName)" + quote + " is null then '' else '\"' + bintostr(convert(varbinary(16384),$(origTableAlias)." + quote + "$(columnName)" + quote + ")) + '\"' end" ;
        imageColumnTemplate = "case when datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ") is null or datalength($(origTableAlias)." + quote + "$(columnName)" + quote + ")=0 then '' else '\"' + bintostr(convert(varbinary(16384),$(origTableAlias)." + quote + "$(columnName)" + quote + ")) + '\"' end" ;
        booleanColumnTemplate = "case when $(tableAlias)." + quote + "$(columnName)" + quote + " is null then '' when $(tableAlias)." + quote + "$(columnName)" + quote + " = 1 then '\"1\"' else '\"0\"' end" ;
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
"                                  $(custom_on_insert_text) " +
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
"                                  $(custom_on_update_text) " +
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
"                                  $(custom_on_delete_text) " +
"                                  set nocount off          " +
"                                end                                                                                                                                                                    " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t where $(whereClause)                                                                                                                                " );
    }

    @Override
    protected String replaceTemplateVariables(DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table originalTable, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {
        ddl = FormatUtils.replace("oldColumns", trigger.isUseCaptureOldData() ?
                super.buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix, table.getColumns(), dml, true, channel, trigger).toString() : "convert(VARCHAR,null)", ddl);
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
                    text += "varchar(max)\n";
                    break;
                case Types.BLOB:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case -10: // SQL-Server ntext binary type
                    text += "varbinary(max)\n";
                    break;
                case Types.OTHER:
                    text += "varbinary(max)\n";
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
