package org.jumpmind.symmetric.db.derby;

import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import org.jumpmind.db.model.Column;
import org.jumpmind.symmetric.db.AbstractTriggerTemplate;
import org.jumpmind.symmetric.db.ISymmetricDialect;

public class DerbyTriggerTemplate extends AbstractTriggerTemplate {

    public DerbyTriggerTemplate(ISymmetricDialect symmetricDialect) {
        super(symmetricDialect);
        //@formatter:off
        functionInstalledSql = "select count(*) from sys.sysaliases where alias = upper('$(functionName)')" ;
        emptyColumnTemplate = "''" ;
        stringColumnTemplate = "sym_escape($(tableAlias).\"$(columnName)\")" ;
        xmlColumnTemplate = null;
        arrayColumnTemplate = null;
        numberColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || rtrim(char($(tableAlias).\"$(columnName)\")) || '\"' end" ;
        datetimeColumnTemplate = "case when $(tableAlias).\"$(columnName)\" is null then '' else '\"' || rtrim(char($(tableAlias).\"$(columnName)\")) || '\"' end" ;
        timeColumnTemplate = null;
        dateColumnTemplate = null;
        clobColumnTemplate = "sym_clob_to_string('\"$(columnName)\"', '$(schemaName)$(tableName)', $(primaryKeyWhereString) )" ;
        blobColumnTemplate = "sym_blob_to_string('\"$(columnName)\"', '$(schemaName)$(tableName)', $(primaryKeyWhereString) )" ;
        wrappedBlobColumnTemplate = null;
        booleanColumnTemplate = null;
        triggerConcatCharacter = "||" ;
        newTriggerValue = "new" ;
        oldTriggerValue = "old" ;
        oldColumnPrefix = "" ;
        newColumnPrefix = "" ;
        otherColumnTemplate = null;

        functionTemplatesToInstall = new HashMap<String,String>();
        functionTemplatesToInstall.put("escape" ,
"CREATE FUNCTION $(functionName)(STR VARCHAR(10000)) RETURNS                                                                                                                                            " + 
"                                VARCHAR(10000) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                 " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.escape'                                                                                                                " );        
        functionTemplatesToInstall.put("clob_to_string" ,
"CREATE FUNCTION $(functionName)(columnName varchar(50),                                                                                                                                                " + 
"                                tableName varchar(50), whereClause varchar(8000)) RETURNS                                                                                                              " + 
"                                varchar(32672) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA EXTERNAL NAME                                                                                         " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.clobToString'                                                                                                          " );
        functionTemplatesToInstall.put("blob_to_string" ,
"CREATE FUNCTION $(functionName)(columnName varchar(50),                                                                                                                                                " + 
"                                tableName varchar(50), whereClause varchar(8000)) RETURNS                                                                                                              " + 
"                                varchar(32672) PARAMETER STYLE JAVA READS SQL DATA LANGUAGE JAVA EXTERNAL NAME                                                                                         " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.blobToString'                                                                                                          " );
        
        functionTemplatesToInstall.put("transaction_id" ,
"CREATE FUNCTION $(functionName)() RETURNS                                                                                                                                                              " + 
"                                varchar(100) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                   " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.getTransactionId'                                                                                                      " );
        functionTemplatesToInstall.put("sync_triggers_disabled" ,
"CREATE FUNCTION $(functionName)() RETURNS                                                                                                                                                              " + 
"                                integer PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                        " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.isSyncDisabled'                                                                                                        " );
        functionTemplatesToInstall.put("sync_triggers_set_disabled" ,
"CREATE FUNCTION $(functionName)(state integer) RETURNS                                                                                                                                                 " + 
"                                integer PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                        " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.setSyncDisabled'                                                                                                       " );
        functionTemplatesToInstall.put("sync_node_set_disabled" ,
"CREATE FUNCTION $(functionName)(nodeId varchar(50)) RETURNS                                                                                                                                            " + 
"                                varchar(50) PARAMETER STYLE JAVA NO SQL LANGUAGE JAVA EXTERNAL NAME                                                                                                    " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.setSyncNodeDisabled'                                                                                                   " );
        functionTemplatesToInstall.put("save_data" ,
"CREATE PROCEDURE $(functionName)(enabled integer, schemaName varchar(50), prefixName varchar(50),                                                                                                                       " + 
"                                tableName varchar(50), channelName varchar(50), dmlType varchar(1), triggerHistId int,                                                                                 " + 
"                                transactionId varchar(1000), externalData varchar(50), columnNames varchar(32672), pkColumnNames varchar(32672))                                       " + 
"                                PARAMETER STYLE JAVA LANGUAGE JAVA MODIFIES SQL DATA EXTERNAL NAME                                                                                                     " + 
"                                'org.jumpmind.symmetric.db.derby.DerbyFunctions.insertData'                                                                                                            " );


        sqlTemplates = new HashMap<String,String>();
        sqlTemplates.put("insertTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                            \n" + 
" AFTER INSERT ON $(schemaName)$(tableName)                               \n" + 
" REFERENCING NEW AS NEW                                                  \n" + 
" FOR EACH ROW MODE DB2SQL                                                \n" + 
" call $(prefixName)_save_data(                                                   \n" +
"   case when $(syncOnInsertCondition) and $(syncOnIncomingBatchCondition) then 1 else 0 end, \n" + 
"   '$(defaultSchema)', '$(prefixName)', '$(targetTableName)',                      \n" + 
"   '$(channelName)', 'I', $(triggerHistoryId),                           \n" + 
"   $(txIdExpression),                                                    \n" + 
"   $(externalSelect),                                                    \n" + 
"   '$(columnNames)',                                                       \n" + 
"   '$(pkColumnNames)')                                                     \n" );
        
        sqlTemplates.put("updateTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                            \n" + 
" AFTER UPDATE ON $(schemaName)$(tableName)                               \n" + 
" REFERENCING OLD AS OLD NEW AS NEW                                       \n" + 
" FOR EACH ROW MODE DB2SQL                                                \n" + 
" call $(prefixName)_save_data(                                                   \n" + 
"   case when $(syncOnUpdateCondition) and $(syncOnIncomingBatchCondition) then 1 else 0 end, \n" + 
"   '$(defaultSchema)', '$(prefixName)', '$(targetTableName)',                      \n" + 
"   '$(channelName)', 'U', $(triggerHistoryId),                           \n" + 
"   $(txIdExpression),                                                    \n" + 
"   $(externalSelect),                                                    \n" + 
"   '$(columnNames)',                                                       \n" + 
"   '$(pkColumnNames)')                                                     \n" );
        
        sqlTemplates.put("deleteTriggerTemplate" ,
"CREATE TRIGGER $(triggerName)                                            \n" + 
" AFTER DELETE ON $(schemaName)$(tableName)                               \n" + 
" REFERENCING OLD AS OLD                                                  \n" + 
" FOR EACH ROW MODE DB2SQL                                                \n" + 
" call $(prefixName)_save_data(                                                   \n" + 
"   case when $(syncOnDeleteCondition) and $(syncOnIncomingBatchCondition) then 1 else 0 end, \n" + 
"   '$(defaultSchema)', '$(prefixName)', '$(targetTableName)',                      \n" + 
"   '$(channelName)', 'D', $(triggerHistoryId),                           \n" + 
"   $(txIdExpression),                                                    \n" + 
"   $(externalSelect),                                                    \n" + 
"   '$(columnNames)',                                                       \n" + 
"   '$(pkColumnNames)')                                                     \n" );
        sqlTemplates.put("initialLoadSqlTemplate" ,
"select $(columns) from $(schemaName)$(tableName) t  where $(whereClause)     " );
        
        //@formatter:on

    }

    @Override
    protected String getPrimaryKeyWhereString(String alias, Column[] columns) {
        List<Column> columnsMinusLobs = new ArrayList<Column>();
        for (Column column : columns) {
            if (!column.isOfBinaryType()) {
                columnsMinusLobs.add(column);
            }
        }

        StringBuilder b = new StringBuilder("'");
        for (Column column : columnsMinusLobs) {
            b.append("\"").append(column.getName()).append("\"=");
            switch (column.getMappedTypeCode()) {
                case Types.BIT:
                case Types.TINYINT:
                case Types.SMALLINT:
                case Types.INTEGER:
                case Types.BIGINT:
                case Types.FLOAT:
                case Types.REAL:
                case Types.DOUBLE:
                case Types.NUMERIC:
                case Types.DECIMAL:
                case Types.BOOLEAN:
                    b.append("'").append(triggerConcatCharacter);
                    b.append("rtrim(char(").append(alias).append(".\"").append(column.getName())
                            .append("\"))");
                    b.append(triggerConcatCharacter).append("'");
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    b.append("'''").append(triggerConcatCharacter);
                    b.append(alias).append(".\"").append(column.getName()).append("\"");
                    b.append(triggerConcatCharacter).append("'''");
                    break;
                case Types.DATE:
                case Types.TIMESTAMP:
                    b.append("{ts '''").append(triggerConcatCharacter);
                    b.append("rtrim(char(").append(alias).append(".\"").append(column.getName())
                            .append("\"))");
                    b.append(triggerConcatCharacter).append("'''}");
                    break;
            }
            if (!column.equals(columnsMinusLobs.get(columnsMinusLobs.size() - 1))) {
                b.append(" and ");
            }
        }
        b.append("'");
        return b.toString();
    }


}