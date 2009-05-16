/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>,
 *               Eric Long <erilong@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.db;

import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.apache.ddlutils.model.Column;
import org.apache.ddlutils.model.Table;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.model.DataEventType;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;

public class SqlTemplate {

    private static final String ORIG_TABLE_ALIAS = "orig";

    static final String INSERT_TRIGGER_TEMPLATE = "insertTriggerTemplate";

    static final String UPDATE_TRIGGER_TEMPLATE = "updateTriggerTemplate";

    static final String DELETE_TRIGGER_TEMPLATE = "deleteTriggerTemplate";

    static final String INITIAL_LOAD_SQL_TEMPLATE = "initialLoadSqlTemplate";

    private Map<String, String> sqlTemplates;

    private Map<String, String> functionTemplatesToInstall;

    private String functionInstalledSql;

    private String triggerPrefix;

    private String stringColumnTemplate;

    private String numberColumnTemplate;

    private String datetimeColumnTemplate;
    
    private String timeColumnTemplate;
    
    public String getTimeColumnTemplate() {
        return timeColumnTemplate;
    }

    public void setTimeColumnTemplate(String timeColumnTemplate) {
        this.timeColumnTemplate = timeColumnTemplate;
    }

    public String getDateColumnTemplate() {
        return dateColumnTemplate;
    }

    public void setDateColumnTemplate(String dateColumnTemplate) {
        this.dateColumnTemplate = dateColumnTemplate;
    }

    private String dateColumnTemplate;

    private String clobColumnTemplate;

    private String blobColumnTemplate;

    private String booleanColumnTemplate;

    private String triggerConcatCharacter;

    private String newTriggerValue;

    private String oldTriggerValue;

    private String oldColumnPrefix = "";

    private String newColumnPrefix = "";

    public String createInitalLoadSql(Node node, IDbDialect dialect, Trigger trig, Table metaData) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = trig.orderColumnsForTable(metaData);
        String columnsText = buildColumnString(dialect.getInitialLoadTableAlias(), dialect.getInitialLoadTableAlias(),
                "", columns).columnString;
        sql = replace("columns", columnsText, sql);

        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName", trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() + "." : "", sql);
        sql = replace("whereClause", trig.getInitialLoadSelect() == null ? "1=1" : trig.getInitialLoadSelect(), sql);
        sql = replace("primaryKeyWhereString", getPrimaryKeyWhereString(dialect.getInitialLoadTableAlias(), metaData
                .getPrimaryKeyColumns()), sql);

        // Replace these parameters to give the initiaLoadContition a chance to
        // reference the node that is being loaded
        sql = replace("groupId", node.getNodeGroupId(), sql);
        sql = replace("externalId", node.getExternalId(), sql);
        sql = replace("nodeId", node.getNodeId(), sql);

        return sql;
    }

    public String createPurgeSql(Node node, IDbDialect dialect, Trigger trig, TriggerHistory hist) {
        // TODO: during reload, purge table using initial_load_select clause
        String sql = "delete from " + getDefaultTargetTableName(trig, hist);
        // + " where " + trig.getInitialLoadSelect();
        // sql = replace("groupId", node.getNodeGroupId(), sql);
        // sql = replace("externalId", node.getExternalId(), sql);
        return sql;
    }

    public String createCsvDataSql(IDbDialect dialect, Trigger trig, Table metaData, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = trig.orderColumnsForTable(metaData);
        String columnsText = buildColumnString(dialect.getInitialLoadTableAlias(), dialect.getInitialLoadTableAlias(),
                "", columns).columnString;
        sql = replace("columns", columnsText, sql);

        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName", trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() + "." : "", sql);
        sql = replace("whereClause", whereClause, sql);
        sql = replace("primaryKeyWhereString", getPrimaryKeyWhereString(dialect.getInitialLoadTableAlias(), metaData
                .getPrimaryKeyColumns()), sql);

        return sql;
    }

    public String createCsvPrimaryKeySql(IDbDialect dialect, Trigger trig, Table metaData, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = metaData.getPrimaryKeyColumns();
        String columnsText = buildColumnString(dialect.getInitialLoadTableAlias(), dialect.getInitialLoadTableAlias(),
                "", columns).columnString;
        sql = replace("columns", columnsText, sql);

        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName", trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() + "." : "", sql);
        sql = replace("whereClause", whereClause, sql);
        sql = replace("primaryKeyWhereString", getPrimaryKeyWhereString(dialect.getInitialLoadTableAlias(), columns),
                sql);

        return sql;
    }

    public String[] getFunctionsToInstall() {
        if (functionTemplatesToInstall != null) {
            return functionTemplatesToInstall.keySet().toArray(new String[functionTemplatesToInstall.size()]);
        } else {
            return new String[0];
        }
    }

    /**
     * TODO Document all the 'templated' values available for building triggers.
     */
    public String createTriggerDDL(IDbDialect dialect, DataEventType dml, Trigger trigger, TriggerHistory history,
            String tablePrefix, Table metaData, String defaultCatalog, String defaultSchema) {

        String ddl = sqlTemplates.get(dml.name().toLowerCase() + "TriggerTemplate");
        if (ddl == null) {
            throw new NotImplementedException(dml.name() + " trigger is not implemented for "
                    + dialect.getPlatform().getName());
        }
        return replaceTemplateVariables(dialect, dml, trigger, history, tablePrefix, metaData, defaultCatalog,
                defaultSchema, ddl);
    }

    public String createPostTriggerDDL(IDbDialect dialect, DataEventType dml, Trigger trigger, TriggerHistory history,
            String tablePrefix, Table metaData, String defaultCatalog, String defaultSchema) {

        String ddl = sqlTemplates.get(dml.name().toLowerCase() + "PostTriggerTemplate");
        return replaceTemplateVariables(dialect, dml, trigger, history, tablePrefix, metaData, defaultCatalog,
                defaultSchema, ddl);
    }

    private String getDefaultTargetTableName(Trigger trigger, TriggerHistory history) {
        String targetTableName = null;
        if (StringUtils.isBlank(trigger.getTargetTableName())) {
            if (history != null) {
                targetTableName = history.getSourceTableName();
            } else {
                targetTableName = trigger.getSourceTableName();
            }
        } else {
            targetTableName = trigger.getTargetTableName();
        }
        return targetTableName;
    }

    public String replaceTemplateVariables(IDbDialect dialect, DataEventType dml, Trigger trigger,
            TriggerHistory history, String tablePrefix, Table metaData, String defaultCatalog, String defaultSchema,
            String ddl) {

        boolean resolveSchemaAndCatalogs = trigger.getSourceCatalogName() != null
                || trigger.getSourceSchemaName() != null;

        ddl = replace("targetTableName", getDefaultTargetTableName(trigger, history), ddl);

        ddl = replace("defaultSchema",
                resolveSchemaAndCatalogs && defaultSchema != null && defaultSchema.length() > 0 ? defaultSchema + "."
                        : "", ddl);
        ddl = replace("defaultCatalog", resolveSchemaAndCatalogs && defaultCatalog != null
                && defaultCatalog.length() > 0 ? defaultCatalog + "." : "", ddl);

        ddl = replace("triggerName", dialect.getTriggerName(dml, triggerPrefix, dialect.getMaxTriggerNameLength(),
                trigger, history).toUpperCase(), ddl);
        ddl = replace("engineName", dialect.getEngineName(), ddl);
        ddl = replace("prefixName", tablePrefix, ddl);
        ddl = replace("targetGroupId", trigger.getTargetGroupId(), ddl);
        ddl = replace("channelName", trigger.getChannelId(), ddl);
        ddl = replace("triggerHistoryId", Integer.toString(history == null ? -1 : history.getTriggerHistoryId()), ddl);
        String triggerExpression = dialect.getTransactionTriggerExpression(defaultCatalog, defaultSchema, trigger);
        if (dialect.isTransactionIdOverrideSupported() && trigger.getTxIdExpression() != null) {
            triggerExpression = trigger.getTxIdExpression();
        }
        ddl = replace("txIdExpression", dialect.preProcessTriggerSqlClause(triggerExpression), ddl);
        ddl = replace("nodeSelectWhere", dialect.preProcessTriggerSqlClause(trigger.getNodeSelect()), ddl);
        ddl = replace("nodeSelectWhereEscaped", replace("'", "''", trigger.getNodeSelect()), ddl);
        ddl = replace("syncOnInsertCondition", dialect.preProcessTriggerSqlClause(trigger.getSyncOnInsertCondition()),
                ddl);
        ddl = replace("syncOnUpdateCondition", dialect.preProcessTriggerSqlClause(trigger.getSyncOnUpdateCondition()),
                ddl);
        ddl = replace("syncOnDeleteCondition", dialect.preProcessTriggerSqlClause(trigger.getSyncOnDeleteCondition()),
                ddl);
        ddl = replace("syncOnIncomingBatchCondition", trigger.isSyncOnIncomingBatch() ? "1=1" : replace("defaultCatalog",
                resolveSchemaAndCatalogs && defaultCatalog != null && defaultCatalog.length() > 0 ? defaultCatalog + "." : "",
                dialect.getSyncTriggersExpression()), ddl);
        ddl = replace("origTableAlias", ORIG_TABLE_ALIAS, ddl);

        Column[] columns = trigger.orderColumnsForTable(metaData);
        ColumnString columnString = buildColumnString(ORIG_TABLE_ALIAS, newTriggerValue, newColumnPrefix, columns);
        ddl = replace("columns", columnString.columnString, ddl);
        ddl = replace("virtualOldNewTable", buildVirtualTableSql(dialect, oldColumnPrefix, newColumnPrefix, metaData.getColumns()),
                ddl);
        String oldColumnString = "null";
        if (trigger.isSyncColumnLevel()) {
            oldColumnString = buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix, columns).columnString;
        } 
        
        ddl = replace("oldColumns", oldColumnString, ddl);
        ddl = eval(columnString.isBlobClob, "containsBlobClobColumns", ddl);

        // some column templates need tableName and schemaName
        ddl = replace("tableName", history == null ? trigger.getSourceTableName() : history.getSourceTableName(), ddl);
        ddl = replace("schemaName", (history == null ? (resolveSchemaAndCatalogs
                && trigger.getSourceSchemaName() != null ? trigger.getSourceSchemaName() + "." : "")
                : (resolveSchemaAndCatalogs && history.getSourceSchemaName() != null ? history.getSourceSchemaName()
                        + "." : "")), ddl);

        columns = metaData.getPrimaryKeyColumns();
        oldColumnString = buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix, columns).columnString;
        ddl = replace("oldKeys", oldColumnString, ddl);
        ddl = replace("oldNewPrimaryKeyJoin", aliasedPrimaryKeyJoin(oldTriggerValue, newTriggerValue, columns), ddl);
        ddl = replace("tableNewPrimaryKeyJoin", aliasedPrimaryKeyJoin(ORIG_TABLE_ALIAS, newTriggerValue, columns), ddl);
        ddl = replace("primaryKeyWhereString", getPrimaryKeyWhereString(newTriggerValue, columns), ddl);

        ddl = replace("declareOldKeyVariables", buildKeyVariablesDeclare(columns, "old"), ddl);
        ddl = replace("declareNewKeyVariables", buildKeyVariablesDeclare(columns, "new"), ddl);
        ddl = replace("oldKeyNames", buildColumnNameString(oldTriggerValue, columns), ddl);
        ddl = replace("newKeyNames", buildColumnNameString(newTriggerValue, columns), ddl);
        ddl = replace("oldKeyVariables", buildKeyVariablesString(columns, "old"), ddl);
        ddl = replace("newKeyVariables", buildKeyVariablesString(columns, "new"), ddl);
        ddl = replace("varNewPrimaryKeyJoin", aliasedPrimaryKeyJoinVar(newTriggerValue, "new", columns), ddl);
        ddl = replace("varOldPrimaryKeyJoin", aliasedPrimaryKeyJoinVar(oldTriggerValue, "old", columns), ddl);

        // replace $(newTriggerValue) and $(oldTriggerValue)
        ddl = replace("newTriggerValue", newTriggerValue, ddl);
        ddl = replace("oldTriggerValue", oldTriggerValue, ddl);
        ddl = replace("newColumnPrefix", newColumnPrefix, ddl);
        ddl = replace("oldColumnPrefix", oldColumnPrefix, ddl);
        switch (dml) {
        case DELETE:
            ddl = replace("curTriggerValue", oldTriggerValue, ddl);
            ddl = replace("curColumnPrefix", oldColumnPrefix, ddl);
            break;
        case INSERT:
        case UPDATE:
        default:
            ddl = replace("curTriggerValue", newTriggerValue, ddl);
            ddl = replace("curColumnPrefix", newColumnPrefix, ddl);
            break;
        }
        return ddl;
    }

    private String buildVirtualTableSql(IDbDialect dialect, String oldTriggerValue, String newTriggerValue,
            Column[] columns) {
        if (oldTriggerValue.indexOf(".") >= 0) {
            oldTriggerValue = oldTriggerValue.substring(oldTriggerValue.indexOf(".") + 1);
        }

        if (newTriggerValue.indexOf(".") >= 0) {
            newTriggerValue = newTriggerValue.substring(newTriggerValue.indexOf(".") + 1);
        }

        StringBuilder b = new StringBuilder("(SELECT ");
        for (Column columnType : columns) {
            String column = columnType.getName();
            b.append("? as ");
            b.append("\"").append(newTriggerValue).append(column).append("\",");
        }

        for (Column columnType : columns) {
            String column = columnType.getName();
            b.append("? AS ");
            b.append("\"").append(oldTriggerValue).append(column).append("\",");
        }
        b.deleteCharAt(b.length() - 1);
        b.append(" FROM DUAL) T ");
        return b.toString();
    }

    private String eval(boolean condition, String prop, String ddl) {
        if (ddl != null) {
            String ifStmt = "$(if:" + prop + ")";
            String elseStmt = "$(else:" + prop + ")";
            String endStmt = "$(end:" + prop + ")";
            int ifIndex = ddl.indexOf(ifStmt);
            if (ifIndex >= 0) {
                int endIndex = ddl.indexOf(endStmt);
                if (endIndex >= 0) {
                    String onTrue = ddl.substring(ifIndex + ifStmt.length(), endIndex);
                    String onFalse = "";
                    int elseIndex = onTrue.indexOf(elseStmt);
                    if (elseIndex >= 0) {
                        onFalse = onTrue.substring(elseIndex + elseStmt.length());
                        onTrue = onTrue.substring(0, elseIndex);
                    }

                    if (condition) {
                        ddl = ddl.substring(0, ifIndex) + onTrue + ddl.substring(endIndex + endStmt.length());
                    } else {
                        ddl = ddl.substring(0, ifIndex) + onFalse + ddl.substring(endIndex + endStmt.length());
                    }

                } else {
                    throw new IllegalStateException(ifStmt + " has to have a " + endStmt);
                }
            }
        }
        return ddl;
    }

    private String aliasedPrimaryKeyJoin(String aliasOne, String aliasTwo, Column[] columns) {
        StringBuilder b = new StringBuilder();
        for (Column column : columns) {
            b.append(aliasOne).append(".\"").append(column.getName()).append("\"");
            b.append("=").append(aliasTwo).append(".\"").append(column.getName()).append("\"");
            if (!column.equals(columns[columns.length - 1])) {
                b.append(" and ");
            }
        }

        return b.toString();
    }

    private String aliasedPrimaryKeyJoinVar(String alias, String prefix, Column[] columns) {
        String text = "";
        for (int i = 0; i < columns.length; i++) {
            text += alias + ".\"" + columns[i].getName() + "\"";
            text += "=@" + prefix + "pk" + i;
            if (i + 1 < columns.length) {
                text += " and ";
            }
        }
        return text;
    }

    // TODO: move to DerbySqlTemplate or change language for use in all DBMSes
    private String getPrimaryKeyWhereString(String alias, Column[] columns) {
        StringBuilder b = new StringBuilder();
        for (Column column : columns) {
            b.append("'\"").append(column.getName()).append("\"=");
            switch (column.getTypeCode()) {
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
                b.append("rtrim(char(").append(alias).append(".\"").append(column.getName()).append("\"))");
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
                b.append("rtrim(char(").append(alias).append(".\"").append(column.getName()).append("\"))");
                b.append(triggerConcatCharacter).append("'''}");
                break;
            }
            if (!column.equals(columns[columns.length - 1])) {
                b.append(" and ");
            }
        }
        b.append("'");
        return b.toString();
    }

    private ColumnString buildColumnString(String origTableAlias, String tableAlias, String columnPrefix, Column[] columns) {
        String columnsText = "";
        boolean isBlobClob = false;
        for (Column column : columns) {
            String templateToUse = null;
            switch (column.getTypeCode()) {
            case Types.TINYINT:
            case Types.SMALLINT:
            case Types.INTEGER:
            case Types.BIGINT:
            case Types.FLOAT:
            case Types.REAL:
            case Types.DOUBLE:
            case Types.NUMERIC:
            case Types.DECIMAL:
                templateToUse = numberColumnTemplate;
                break;
            case Types.CHAR:
            case Types.VARCHAR:
            case Types.LONGVARCHAR:
                templateToUse = stringColumnTemplate;
                break;
            case Types.CLOB:
                templateToUse = clobColumnTemplate;
                isBlobClob = true;
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                // SQL-Server ntext binary type
            case -10:
                templateToUse = blobColumnTemplate;
                isBlobClob = true;
                break;
            case Types.DATE:
                if(noDateColumnTemplate()){
                    templateToUse = datetimeColumnTemplate;
                    break;
                }
                templateToUse = dateColumnTemplate;
                break;
            case Types.TIME:
                if(noTimeColumnTemplate()){
                    templateToUse = datetimeColumnTemplate;
                    break;
                }
                templateToUse = timeColumnTemplate;
                break;
            case Types.TIMESTAMP:
                templateToUse = datetimeColumnTemplate;
                break;
            case Types.BOOLEAN:
            case Types.BIT:
                templateToUse = booleanColumnTemplate;
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

            if (templateToUse != null) {
                templateToUse = templateToUse.trim();
            } else {
                throw new NotImplementedException();
            }

            columnsText = columnsText + "\n          "
                    + replace("columnName", String.format("%s%s", columnPrefix, column.getName()), templateToUse);

        }

        String lastCommandToken = triggerConcatCharacter + "','" + triggerConcatCharacter;

        if (columnsText.endsWith(lastCommandToken)) {
            columnsText = columnsText.substring(0, columnsText.length() - lastCommandToken.length());
        }
        
        // H2 (and maybe other embedded java triggers) escape the ' character so
        // it may be inserted into the
        // database as SQL that can be used by the trigger code.
        lastCommandToken = triggerConcatCharacter + "'',''" + triggerConcatCharacter;

        if (columnsText.endsWith(lastCommandToken)) {
            columnsText = columnsText.substring(0, columnsText.length() - lastCommandToken.length());
        }

        columnsText = replace("origTableAlias", origTableAlias, columnsText);
        columnsText = replace("tableAlias", tableAlias, columnsText);
        return new ColumnString(columnsText, isBlobClob);
    }

    private boolean noTimeColumnTemplate() {
        return timeColumnTemplate == null || timeColumnTemplate.equals("null") || timeColumnTemplate.trim().equals("");
    }

    private boolean noDateColumnTemplate() {
        return dateColumnTemplate == null || dateColumnTemplate.equals("null") || dateColumnTemplate.trim().equals("");
    }

    private String buildColumnNameString(String tableAlias, Column[] columns) {
        String columnsText = "";
        for (int i = 0; i < columns.length; i++) {
            columnsText += tableAlias + ".\"" + columns[i].getName() + "\"";
            if (i + 1 < columns.length) {
                columnsText += ", ";
            }
        }
        return columnsText;
    }

    private String buildKeyVariablesDeclare(Column[] columns, String prefix) {
        String text = "";
        for (int i = 0; i < columns.length; i++) {
            text += "declare @" + prefix + "pk" + i + " ";
            switch (columns[i].getTypeCode()) {
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
            case Types.NULL:
            case Types.OTHER:
            case Types.JAVA_OBJECT:
            case Types.DISTINCT:
            case Types.STRUCT:
            case Types.REF:
            case Types.DATALINK:
                throw new NotImplementedException(columns[i] + " is of type " + columns[i].getType());
            }
        }

        return text;
    }

    private String buildKeyVariablesString(Column[] columns, String prefix) {
        String text = "";
        for (int i = 0; i < columns.length; i++) {
            text += "@" + prefix + "pk" + i;
            if (i + 1 < columns.length) {
                text += ", ";
            }
        }
        return text;
    }

    private String replace(String prop, String replaceWith, String sourceString) {
        return StringUtils.replace(sourceString, "$(" + prop + ")", replaceWith);
    }

    public void setStringColumnTemplate(String columnTemplate) {
        this.stringColumnTemplate = columnTemplate;
    }

    public void setDatetimeColumnTemplate(String datetimeColumnTemplate) {
        this.datetimeColumnTemplate = datetimeColumnTemplate;
    }

    public void setNumberColumnTemplate(String numberColumnTemplate) {
        this.numberColumnTemplate = numberColumnTemplate;
    }

    public void setSqlTemplates(Map<String, String> sqlTemplates) {
        this.sqlTemplates = sqlTemplates;
    }

    public String getClobColumnTemplate() {
        return clobColumnTemplate;
    }

    public void setClobColumnTemplate(String clobColumnTemplate) {
        this.clobColumnTemplate = clobColumnTemplate;
    }

    public void setBooleanColumnTemplate(String booleanColumnTemplate) {
        this.booleanColumnTemplate = booleanColumnTemplate;
    }

    public void setTriggerConcatCharacter(String triggerConcatCharacter) {
        this.triggerConcatCharacter = triggerConcatCharacter;
    }

    public String getNewTriggerValue() {
        return newTriggerValue;
    }

    public void setNewTriggerValue(String newTriggerValue) {
        this.newTriggerValue = newTriggerValue;
    }

    public String getOldTriggerValue() {
        return oldTriggerValue;
    }

    public void setOldTriggerValue(String oldTriggerValue) {
        this.oldTriggerValue = oldTriggerValue;
    }

    public void setTriggerPrefix(String triggerPrefix) {
        this.triggerPrefix = triggerPrefix;
    }

    public String getBlobColumnTemplate() {
        return blobColumnTemplate;
    }

    public void setBlobColumnTemplate(String blobColumnTemplate) {
        this.blobColumnTemplate = blobColumnTemplate;
    }

    public void setFunctionInstalledSql(String functionInstalledSql) {
        this.functionInstalledSql = functionInstalledSql;
    }

    public void setFunctionTemplatesToInstall(Map<String, String> functionTemplatesToInstall) {
        this.functionTemplatesToInstall = functionTemplatesToInstall;
    }

    public void setOldColumnPrefix(String oldColumnPrefix) {
        this.oldColumnPrefix = oldColumnPrefix;
    }

    public void setNewColumnPrefix(String newColumnPrefix) {
        this.newColumnPrefix = newColumnPrefix;
    }

    public String getFunctionSql(String functionName) {
        if (this.functionTemplatesToInstall != null) {
            String ddl = replace("functionName", functionName, this.functionTemplatesToInstall.get(functionName));
            ddl = replace("version", Version.versionWithUnderscores(), ddl);
            return ddl;
        } else {
            return null;
        }
    }

    public String getFunctionInstalledSql(String functionName) {
        if (functionInstalledSql != null) {
            String ddl = replace("functionName", functionName, functionInstalledSql);
            ddl = replace("version", Version.versionWithUnderscores(), ddl);
            return ddl;
        } else {
            return null;
        }
    }
    
    private class ColumnString {
        
        String columnString;
        boolean isBlobClob = false;
        
        ColumnString(String columnExpression, boolean isBlobClob) {
            this.columnString = columnExpression;
            this.isBlobClob = isBlobClob;
        }

    }

}
