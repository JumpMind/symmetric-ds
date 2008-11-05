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

    private String clobColumnTemplate;

    private String blobColumnTemplate;

    private String booleanColumnTemplate;

    private String triggerConcatCharacter;

    private String newTriggerValue;

    private String oldTriggerValue;

    public String createInitalLoadSql(Node node, IDbDialect dialect, Trigger trig, Table metaData) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = trig.orderColumnsForTable(metaData);
        String columnsText = buildColumnString("t", "t", columns);
        sql = replace("columns", columnsText, sql);

        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName", trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() + "." : "", sql);
        sql = replace("whereClause", trig.getInitialLoadSelect() == null ? "1=1" : trig.getInitialLoadSelect(), sql);
        sql = replace("primaryKeyWhereString", getPrimaryKeyWhereString("t", metaData.getPrimaryKeyColumns()), sql);

        // Replace these parameters to give the initiaLoadContition a chance to
        // reference domainNames and domainIds
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

    public String createCsvDataSql(Trigger trig, Table metaData, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = trig.orderColumnsForTable(metaData);
        String columnsText = buildColumnString("t", "t", columns);
        sql = replace("columns", columnsText, sql);

        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName", trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() + "." : "", sql);
        sql = replace("whereClause", whereClause, sql);
        sql = replace("primaryKeyWhereString", getPrimaryKeyWhereString("t", metaData.getPrimaryKeyColumns()), sql);

        return sql;
    }

    public String createCsvPrimaryKeySql(Trigger trig, Table metaData, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = metaData.getPrimaryKeyColumns();
        String columnsText = buildColumnString("t", "t", columns);
        sql = replace("columns", columnsText, sql);

        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName", trig.getSourceSchemaName() != null ? trig.getSourceSchemaName() + "." : "", sql);
        sql = replace("whereClause", whereClause, sql);
        sql = replace("primaryKeyWhereString", getPrimaryKeyWhereString("t", columns), sql);

        return sql;
    }

    public String[] getFunctionsToInstall() {
        if (functionTemplatesToInstall != null) {
            return functionTemplatesToInstall.keySet().toArray(new String[functionTemplatesToInstall.size()]);
        } else {
            return new String[0];
        }
    }

    public String createFunctionDDL(String name) {
        if (functionTemplatesToInstall != null) {
            return functionTemplatesToInstall.get(name);
        } else {
            return null;
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
            targetTableName = history.getSourceTableName();
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

        ddl = replace("triggerName", trigger.getTriggerName(dml, triggerPrefix, dialect.getMaxTriggerNameLength())
                .toUpperCase(), ddl);
        ddl = replace("engineName", dialect.getEngineName(), ddl);
        ddl = replace("prefixName", tablePrefix, ddl);
        ddl = replace("targetGroupId", trigger.getTargetGroupId(), ddl);
        ddl = replace("channelName", trigger.getChannelId(), ddl);
        ddl = replace("triggerHistoryId", Integer.toString(history.getTriggerHistoryId()), ddl);
        String triggerExpression = dialect.getTransactionTriggerExpression(trigger);
        if (dialect.isTransactionIdOverrideSupported() && trigger.getTxIdExpression() != null) {
            triggerExpression = trigger.getTxIdExpression();
        }
        ddl = replace("txIdExpression", triggerExpression, ddl);
        ddl = replace("nodeSelectWhere", trigger.getNodeSelect(), ddl);
        ddl = replace("nodeSelectWhereEscaped", replace("'", "''", trigger.getNodeSelect()), ddl);
        ddl = replace("syncOnInsertCondition", trigger.getSyncOnInsertCondition(), ddl);
        ddl = replace("syncOnUpdateCondition", trigger.getSyncOnUpdateCondition(), ddl);
        ddl = replace("syncOnDeleteCondition", trigger.getSyncOnDeleteCondition(), ddl);
        ddl = replace("syncOnIncomingBatchCondition", trigger.isSyncOnIncomingBatch() ? "1=1" : dialect
                .getSyncTriggersExpression(), ddl);
        ddl = replace("origTableAlias", ORIG_TABLE_ALIAS, ddl);

        Column[] columns = trigger.orderColumnsForTable(metaData);
        String columnsText = buildColumnString(ORIG_TABLE_ALIAS, newTriggerValue, columns);
        ddl = replace("columns", columnsText, ddl);
        if (trigger.isSyncColumnLevel()) {
            columnsText = buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, columns);
        } else {
            columnsText = "null";
        }
        ddl = replace("oldColumns", columnsText, ddl);
        ddl = eval(containsBlobClobColumns(columns), "containsBlobClobColumns", ddl);

        // some column templates need tableName and schemaName
        ddl = replace("tableName", history.getSourceTableName(), ddl);
        ddl = replace("schemaName", resolveSchemaAndCatalogs && history.getSourceSchemaName() != null ? history
                .getSourceSchemaName()
                + "." : "", ddl);

        columns = metaData.getPrimaryKeyColumns();
        columnsText = buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, columns);
        ddl = replace("oldKeys", columnsText, ddl);
        ddl = replace("oldNewPrimaryKeyJoin", aliasedPrimaryKeyJoin(oldTriggerValue, newTriggerValue, columns), ddl);
        ddl = replace("tableNewPrimaryKeyJoin", aliasedPrimaryKeyJoin(ORIG_TABLE_ALIAS, newTriggerValue, columns), ddl);
        ddl = replace("primaryKeyWhereString", getPrimaryKeyWhereString(newTriggerValue, columns), ddl);

        // replace $(newTriggerValue) and $(oldTriggerValue)
        ddl = replace("newTriggerValue", newTriggerValue, ddl);
        ddl = replace("oldTriggerValue", oldTriggerValue, ddl);
        switch (dml) {
        case DELETE:
            ddl = replace("curTriggerValue", oldTriggerValue, ddl);
            break;
        case INSERT:
        case UPDATE:
        default:
            ddl = replace("curTriggerValue", newTriggerValue, ddl);
            break;
        }
        return ddl;
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

    private boolean containsBlobClobColumns(Column[] columns) {
        for (Column column : columns) {
            switch (column.getTypeCode()) {
            case Types.CLOB:
            case Types.BLOB:
            case Types.BINARY:
                return true;
            }
        }
        return false;
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

    private String buildColumnString(String origTableAlias, String tableAlias, Column[] columns) {
        String columnsText = "";
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
                break;
            case Types.BLOB:
            case Types.BINARY:
            case Types.VARBINARY:
            case Types.LONGVARBINARY:
                templateToUse = blobColumnTemplate;
                break;
            case Types.DATE:
            case Types.TIME:
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

            columnsText = columnsText + "\n          " + replace("columnName", column.getName(), templateToUse);

        }

        String LAST_COMMAN_TOKEN = triggerConcatCharacter + "','" + triggerConcatCharacter;

        if (columnsText.endsWith(LAST_COMMAN_TOKEN)) {
            columnsText = columnsText.substring(0, columnsText.length() - LAST_COMMAN_TOKEN.length());
        }

        columnsText = replace("origTableAlias", origTableAlias, columnsText);
        return replace("tableAlias", tableAlias, columnsText);

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

    public String getFunctionSql(String functionName) {
        if (this.functionTemplatesToInstall != null) {
            return this.functionTemplatesToInstall.get(functionName);
        } else {
            return null;
        }
    }

    public String getFunctionInstalledSql(String functionName) {
        if (functionInstalledSql != null) {
            String ddl = replace("functionName", functionName, functionInstalledSql);
            return ddl;
        } else {
            return null;
        }
    }
}
