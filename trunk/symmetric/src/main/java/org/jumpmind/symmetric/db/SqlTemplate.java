
package org.jumpmind.symmetric.db;

import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.Trigger;

public class SqlTemplate
{

    static final String INSERT_TRIGGER_TEMPLATE = "insertTriggerTemplate";

    static final String UPDATE_TRIGGER_TEMPLATE = "updateTriggerTemplate";

    static final String DELETE_TRIGGER_TEMPLATE = "deleteTriggerTemplate";

    static final String INITIAL_LOAD_SQL_TEMPLATE = "initialLoadSqlTemplate";

    Map<String, String> sqlTemplates;

    String stringColumnTemplate;

    String numberColumnTemplate;

    String datetimeColumnTemplate;

    String clobColumnTemplate;

    String triggerConcatCharacter;

    String newTriggerValue;

    String oldTriggerValue;

    public String createInitalLoadSql(Node node, IDbDialect dialect, Trigger config, Table metaData)
    {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);
        sql = replace("tableName", config.getSourceTableName(), sql);
        sql = replace("schemaName", config.getSourceSchemaName() != null ? config.getSourceSchemaName() + "." : "", sql);
        sql = replace("initialLoadCondition", config.getInitialLoadSelect() == null ? "1=1" : config
            .getInitialLoadSelect(), sql);

        // Replace these parameters to give the initiaLoadContition a chance to reference domainNames and domainIds
        sql = replace("groupId", node.getGroupId(), sql);
        sql = replace("externalId", node.getExternalId(), sql);

        Column[] columns = config.orderColumnsForTable(metaData);
        String columnsText = buildColumnString("t", columns);
        sql = replace("columns", columnsText, sql);
        return sql;
    }

    public String createTriggerDDL(IDbDialect dialect, DataEventType dml, Trigger config, TriggerHistory audit,
        String tablePrefix, Table metaData, String defaultSchema)
    {

        String ddl = sqlTemplates.get(dml.name().toLowerCase() + "TriggerTemplate");
        if (ddl == null)
        {
            throw new NotImplementedException(dml.name() + " trigger is not implemented for "
                + dialect.getPlatform().getName());
        }
        ddl = replace("tableName", config.getSourceTableName().toUpperCase(), ddl);
        ddl = replace("schemaName", config.getSourceSchemaName() != null ? config.getSourceSchemaName().toUpperCase() + "." : "",
            ddl);
        ddl = replace("defaultSchema", defaultSchema != null && defaultSchema.length() > 0 ? defaultSchema + "." : "",
            ddl);
        ddl = replace("triggerName", config.getTriggerName(dml).toUpperCase(), ddl);
        ddl = replace("prefixName", tablePrefix, ddl);
        ddl = replace("targetGroupId", config.getTargetGroupId(), ddl);
        ddl = replace("channelName", config.getChannelId(), ddl);
        ddl = replace("triggerHistoryId", Integer.toString(audit.getTriggerHistoryId()), ddl);
        ddl = replace("txIdExpression", config.getTxIdExpression() == null ? dialect
            .getTransactionTriggerExpression() : config.getTxIdExpression(), ddl);
        ddl = replace("nodeSelectWhere", config.getNodeSelect(), ddl);        
        ddl = replace("syncOnInsertCondition", config.getSyncOnInsertCondition(), ddl);
        ddl = replace("syncOnUpdateCondition", config.getSyncOnUpdateCondition(), ddl);
        ddl = replace("syncOnDeleteCondition", config.getSyncOnDeleteCondition(), ddl);

        Column[] columns = config.orderColumnsForTable(metaData);
        String columnsText = buildColumnString(newTriggerValue, columns);
        ddl = replace("columns", columnsText, ddl);

        columns = metaData.getPrimaryKeyColumns();
        columnsText = buildColumnString(oldTriggerValue, columns);
        ddl = replace("oldKeys", columnsText, ddl);

        return ddl;
    }

    private String buildColumnString(String tableAlias, Column[] columns)
    {
        String columnsText = "";
        for (Column column : columns)
        {
            String templateToUse = null;
            switch (column.getTypeCode())
            {
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
                templateToUse = numberColumnTemplate;
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                templateToUse = stringColumnTemplate;
                break;
            case Types.CLOB:
                templateToUse = clobColumnTemplate;
                break;
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                templateToUse = stringColumnTemplate;
                break;
            case Types.DATE:
            case Types.TIME:
            case Types.TIMESTAMP:
                templateToUse = datetimeColumnTemplate;
                break;
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.DATALINK:
                throw new NotImplementedException(column.getName() + " is of type " + column.getType());
            }

            if (templateToUse != null)
            {
                templateToUse = templateToUse.trim();
            }
            else
            {
                throw new NotImplementedException();
            }

            columnsText = columnsText + "\n          " + replace("columnName", column.getName(), templateToUse);

        }

        String LAST_COMMAN_TOKEN = triggerConcatCharacter + "','" + triggerConcatCharacter;

        if (columnsText.endsWith(LAST_COMMAN_TOKEN))
        {
            columnsText = columnsText.substring(0, columnsText.length() - LAST_COMMAN_TOKEN.length());
        }

        return replace("tableAlias", tableAlias, columnsText);

    }

    private String replace(String prop, String replaceWith, String sourceString)
    {
        String replaceString = "\\$\\(" + prop + "\\)";
        if (sourceString.contains("$(" + prop + ")"))
        {
            return sourceString.replaceAll(replaceString, String.valueOf(replaceWith));
        }
        else
        {
            return sourceString;
        }
    }

    public void setStringColumnTemplate(String columnTemplate)
    {
        this.stringColumnTemplate = columnTemplate;
    }

    public void setDatetimeColumnTemplate(String datetimeColumnTemplate)
    {
        this.datetimeColumnTemplate = datetimeColumnTemplate;
    }

    public void setNumberColumnTemplate(String numberColumnTemplate)
    {
        this.numberColumnTemplate = numberColumnTemplate;
    }

    public void setSqlTemplates(Map<String, String> sqlTemplates)
    {
        this.sqlTemplates = sqlTemplates;
    }

    public String getClobColumnTemplate()
    {
        return clobColumnTemplate;
    }

    public void setClobColumnTemplate(String clobColumnTemplate)
    {
        this.clobColumnTemplate = clobColumnTemplate;
    }

    public void setTriggerConcatCharacter(String triggerConcatCharacter)
    {
        this.triggerConcatCharacter = triggerConcatCharacter;
    }

    public String getNewTriggerValue()
    {
        return newTriggerValue;
    }

    public void setNewTriggerValue(String newTriggerValue)
    {
        this.newTriggerValue = newTriggerValue;
    }

    public String getOldTriggerValue()
    {
        return oldTriggerValue;
    }

    public void setOldTriggerValue(String oldTriggerValue)
    {
        this.oldTriggerValue = oldTriggerValue;
    }
}
