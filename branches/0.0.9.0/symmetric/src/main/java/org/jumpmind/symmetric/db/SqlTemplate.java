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

    static final String INSERT_TRIGGER_TEMPLATE = "insertTriggerTemplate";

    static final String UPDATE_TRIGGER_TEMPLATE = "updateTriggerTemplate";

    static final String DELETE_TRIGGER_TEMPLATE = "deleteTriggerTemplate";

    static final String INITIAL_LOAD_SQL_TEMPLATE = "initialLoadSqlTemplate";

    Map<String, String> sqlTemplates;
    
    String triggerPrefix;

    String stringColumnTemplate;

    String numberColumnTemplate;

    String datetimeColumnTemplate;

    String clobColumnTemplate;
    
    String blobColumnTemplate;

    String triggerConcatCharacter;

    String newTriggerValue;

    String oldTriggerValue;

    public String createInitalLoadSql(Node node, IDbDialect dialect,
            Trigger trig, Table metaData) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);
        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName",
                trig.getSourceSchemaName() != null ? trig
                        .getSourceSchemaName()
                        + "." : "", sql);
        sql = replace("whereClause",
                trig.getInitialLoadSelect() == null ? "1=1" : trig
                        .getInitialLoadSelect(), sql);

        // Replace these parameters to give the initiaLoadContition a chance to reference domainNames and domainIds
        sql = replace("groupId", node.getNodeGroupId(), sql);
        sql = replace("externalId", node.getExternalId(), sql);

        Column[] columns = trig.orderColumnsForTable(metaData);
        String columnsText = buildColumnString("t", columns);
        sql = replace("columns", columnsText, sql);
        return sql;
    }

    public String createPurgeSql(Node node, IDbDialect dialect, Trigger trig) {
        // TODO: during reload, purge table using initial_load_select clause
        String sql = "delete from " + trig.getDefaultTargetTableName();
        //+ " where " + trig.getInitialLoadSelect();
        //sql = replace("groupId", node.getNodeGroupId(), sql);
        //sql = replace("externalId", node.getExternalId(), sql);
        return sql;
    }

    public String createCsvDataSql(Trigger trig, Table metaData, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);
        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName",
                trig.getSourceSchemaName() != null ? trig
                        .getSourceSchemaName()
                        + "." : "", sql);
        sql = replace("whereClause", whereClause, sql);

        Column[] columns = trig.orderColumnsForTable(metaData);
        String columnsText = buildColumnString("t", columns);
        sql = replace("columns", columnsText, sql);
        return sql;
    }
    
    public String createCsvPrimaryKeySql(Trigger trig, Table metaData, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);
        sql = replace("tableName", trig.getSourceTableName(), sql);
        sql = replace("schemaName",
                trig.getSourceSchemaName() != null ? trig
                        .getSourceSchemaName()
                        + "." : "", sql);
        sql = replace("whereClause", whereClause, sql);

        Column[] columns = metaData.getPrimaryKeyColumns();
        String columnsText = buildColumnString("t", columns);
        sql = replace("columns", columnsText, sql);
        return sql;
    } 

    public String createTriggerDDL(IDbDialect dialect, DataEventType dml,
            Trigger trigger, TriggerHistory history, String tablePrefix,
            Table metaData, String defaultSchema) {

        String ddl = sqlTemplates.get(dml.name().toLowerCase()
                + "TriggerTemplate");
        if (ddl == null) {
            throw new NotImplementedException(dml.name()
                    + " trigger is not implemented for "
                    + dialect.getPlatform().getName());
        }
        ddl = replace("tableName", trigger.getSourceTableName().toUpperCase(),
                ddl);
        ddl = replace("targetTableName", trigger.getDefaultTargetTableName().toUpperCase(),
                ddl);        
        ddl = replace("schemaName",
                trigger.getSourceSchemaName() != null ? trigger
                        .getSourceSchemaName().toUpperCase()
                        + "." : "", ddl);
        ddl = replace("defaultSchema", defaultSchema != null
                && defaultSchema.length() > 0 ? defaultSchema + "." : "", ddl);
        ddl = replace("triggerName", trigger.getTriggerName(dml, triggerPrefix).toUpperCase(),
                ddl);
        ddl = replace("prefixName", tablePrefix, ddl);
        ddl = replace("targetGroupId", trigger.getTargetGroupId(), ddl);
        ddl = replace("channelName", trigger.getChannelId(), ddl);
        ddl = replace("triggerHistoryId", Integer.toString(history
                .getTriggerHistoryId()), ddl);
        ddl = replace("txIdExpression",
                trigger.getTxIdExpression() == null ? dialect
                        .getTransactionTriggerExpression() : trigger
                        .getTxIdExpression(), ddl);
        ddl = replace("nodeSelectWhere", trigger.getNodeSelect(), ddl);
        ddl = replace("syncOnInsertCondition", trigger
                .getSyncOnInsertCondition(), ddl);
        ddl = replace("syncOnUpdateCondition", trigger
                .getSyncOnUpdateCondition(), ddl);
        ddl = replace("syncOnDeleteCondition", trigger
                .getSyncOnDeleteCondition(), ddl);

        Column[] columns = trigger.orderColumnsForTable(metaData);
        String columnsText = buildColumnString(newTriggerValue, columns);
        ddl = replace("columns", columnsText, ddl);

        columns = metaData.getPrimaryKeyColumns();
        columnsText = buildColumnString(oldTriggerValue, columns);
        ddl = replace("oldKeys", columnsText, ddl);

        return ddl;
    }

    private String buildColumnString(String tableAlias, Column[] columns) {
        String columnsText = "";
        for (Column column : columns) {
            String templateToUse = null;
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
                templateToUse = blobColumnTemplate;
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
                throw new NotImplementedException(column.getName()
                        + " is of type " + column.getType());
            }

            if (templateToUse != null) {
                templateToUse = templateToUse.trim();
            } else {
                throw new NotImplementedException();
            }

            columnsText = columnsText + "\n          "
                    + replace("columnName", column.getName(), templateToUse);

        }

        String LAST_COMMAN_TOKEN = triggerConcatCharacter + "','"
                + triggerConcatCharacter;

        if (columnsText.endsWith(LAST_COMMAN_TOKEN)) {
            columnsText = columnsText.substring(0, columnsText.length()
                    - LAST_COMMAN_TOKEN.length());
        }

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
}
