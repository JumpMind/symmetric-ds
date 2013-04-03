package org.jumpmind.symmetric.db.mssql2000;

import java.sql.Types;
import java.util.HashMap;

import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.db.model.Column;
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
            "  end                                                                                                                                                                    " );

        sqlTemplates.put("initialLoadSqlTemplate" ,
                "select $(columns) from $(schemaName)$(tableName) t where $(whereClause) " );

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
                    text += "bigint\n";
                    break;
                case Types.NUMERIC:
                case Types.DECIMAL:
                    text += "decimal\n";
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
                    text += "varchar(8000)\n";
                    break;
                case Types.BLOB:
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                case -10: // SQL-Server ntext binary type
                    text += "varbinary(8000)\n";
                    break;
                case Types.OTHER:
                    text += "varbinary(8000)\n";
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
