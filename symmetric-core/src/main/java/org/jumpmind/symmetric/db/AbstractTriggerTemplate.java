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
package org.jumpmind.symmetric.db;

import static org.apache.commons.lang.StringUtils.isBlank;
import static org.apache.commons.lang.StringUtils.isNotBlank;

import java.lang.reflect.Field;
import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.ColumnTypes;
import org.jumpmind.db.model.Table;
import org.jumpmind.db.model.TypeMap;
import org.jumpmind.db.sql.DmlStatement.DmlType;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.SymmetricUtils;
import org.jumpmind.util.FormatUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Responsible for generating dialect specific SQL such as trigger bodies and
 * functions
 */
abstract public class AbstractTriggerTemplate {

    protected final Logger log = LoggerFactory.getLogger(getClass());
    
    protected static final String ORIG_TABLE_ALIAS = "orig";

    static final String INSERT_TRIGGER_TEMPLATE = "insertTriggerTemplate";

    static final String UPDATE_TRIGGER_TEMPLATE = "updateTriggerTemplate";

    static final String DELETE_TRIGGER_TEMPLATE = "deleteTriggerTemplate";

    static final String INITIAL_LOAD_SQL_TEMPLATE = "initialLoadSqlTemplate";

    protected Map<String, String> sqlTemplates;

    protected String emptyColumnTemplate = "''";

    protected String stringColumnTemplate;

    protected String xmlColumnTemplate;

    protected String arrayColumnTemplate;

    protected String numberColumnTemplate;

    protected String datetimeColumnTemplate;

    protected String timeColumnTemplate;

    protected String dateColumnTemplate;

    protected String dateTimeWithTimeZoneColumnTemplate;
    
    protected String dateTimeWithLocalTimeZoneColumnTemplate;

    protected String geometryColumnTemplate;
    
    protected String geographyColumnTemplate;

    protected String clobColumnTemplate;

    protected String blobColumnTemplate;
    
    protected String binaryColumnTemplate;

    protected String imageColumnTemplate;

    protected String wrappedBlobColumnTemplate;

    protected String booleanColumnTemplate;

    protected String triggerConcatCharacter;

    protected String newTriggerValue;

    protected String oldTriggerValue;

    protected String oldColumnPrefix = "";

    protected String newColumnPrefix = "";

    protected String otherColumnTemplate;

    protected ISymmetricDialect symmetricDialect;

    protected AbstractTriggerTemplate(ISymmetricDialect symmetricDialect) {
        this.symmetricDialect = symmetricDialect;
    }
    
    /**
     * When {@link ParameterConstants#INITIAL_LOAD_CONCAT_CSV_IN_SQL_ENABLED} is false 
     * most dialects are going to want to still use the trigger templates because they
     * have type translation details (like geometry templates).  However, some dialects
     * cannot handle the complex SQL generated (Firebird).  We needed a way to tell
     * the dialect that we want to select the columns straight up.
     */
    public boolean useTriggerTemplateForColumnTemplatesDuringInitialLoad() {
        return true;
    }

    public String createInitalLoadSql(Node node, TriggerRouter triggerRouter, Table originalTable,
            TriggerHistory triggerHistory, Channel channel, String overrideSelectSql) {

        IParameterService parameterService = symmetricDialect.getParameterService();
        
        boolean dateTimeAsString = parameterService.is(
                ParameterConstants.DATA_LOADER_TREAT_DATETIME_AS_VARCHAR);   
        
        boolean concatInCsv = parameterService.is(
                ParameterConstants.INITIAL_LOAD_CONCAT_CSV_IN_SQL_ENABLED);
        
        Table table = originalTable.copyAndFilterColumns(triggerHistory.getParsedColumnNames(),
                triggerHistory.getParsedPkColumnNames(), true);

        Column[] columns = table.getColumns();

        String textColumnExpression = parameterService.getString(ParameterConstants.DATA_EXTRACTOR_TEXT_COLUMN_EXPRESSION);

        String sql = null;

        String tableAlias = symmetricDialect.getInitialLoadTableAlias();
        
        if (concatInCsv) {
            sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);
            String columnsText = buildColumnsString(tableAlias,
                    tableAlias, "", columns, DataEventType.INSERT,
                    false, channel, triggerRouter.getTrigger()).columnString;
            if (isNotBlank(textColumnExpression)) {
                columnsText = textColumnExpression.replace("$(columnName)", columnsText);
            }            
            sql = FormatUtils.replace("columns", columnsText, sql);
        } else {            
            sql = "select $(columns) from $(schemaName)$(tableName) t where $(whereClause)";
            StringBuilder columnList = new StringBuilder();
            for (int i = 0; i < columns.length; i++) {
                Column column = columns[i];
                if (column != null) {
                    if (i > 0) {
                        columnList.append(",");
                    }
                    boolean isLob = symmetricDialect.getPlatform()
                            .isLob(column.getMappedTypeCode());
                    if (!(isLob && triggerRouter.getTrigger().isUseStreamLobs())) {

                        String columnExpression = null;
                        if (useTriggerTemplateForColumnTemplatesDuringInitialLoad()) {
                            ColumnString columnString = fillOutColumnTemplate(tableAlias,
                                    tableAlias, "", column, DataEventType.INSERT, false, channel,
                                    triggerRouter.getTrigger());
                            columnExpression = columnString.columnString;
                            if (isNotBlank(textColumnExpression)
                                    && TypeMap.isTextType(column.getMappedTypeCode())) {
                                columnExpression = textColumnExpression.replace("$(columnName)",
                                        columnExpression);
                            }
                        } else {
                            columnExpression = SymmetricUtils.quote(symmetricDialect,
                                    column.getName());

                            if (dateTimeAsString
                                    && TypeMap.isDateTimeType(column.getMappedTypeCode())) {
                                columnExpression = castDatetimeColumnToString(column.getName());
                            } else if (isNotBlank(textColumnExpression)
                                    && TypeMap.isTextType(column.getMappedTypeCode())) {
                                columnExpression = textColumnExpression.replace("$(columnName)",
                                        columnExpression);
                            }
                        }
                        
                        columnList.append(columnExpression).append(" as ").append("x__").append(i);
                        
                    } else {
                        columnList.append(" ").append(emptyColumnTemplate).append(" as ").append("x__").append(i);
                    }
                }
            }
            sql = FormatUtils.replace("columns", columnList.toString(), sql);
        }
        
        String initialLoadSelect = StringUtils.isBlank(triggerRouter.getInitialLoadSelect()) ? Constants.ALWAYS_TRUE_CONDITION
                : triggerRouter.getInitialLoadSelect();
        if (StringUtils.isNotBlank(overrideSelectSql)) {
        	initialLoadSelect = overrideSelectSql;
        }
        sql = FormatUtils
                .replace(
                        "whereClause", initialLoadSelect
                        , sql);
        sql = FormatUtils.replace("tableName", SymmetricUtils.quote(symmetricDialect, table.getName()), sql);
        sql = FormatUtils.replace("schemaName",
                triggerHistory == null ? getSourceTablePrefix(originalTable)
                        : getSourceTablePrefix(triggerHistory), sql);
        sql = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(symmetricDialect.getInitialLoadTableAlias(),
                        table.hasPrimaryKey() ? table.getPrimaryKeyColumns() : table.getColumns()),
                sql);

        // Replace these parameters to give the initiaLoadContition a chance to
        // reference the node that is being loaded
        sql = FormatUtils.replace("groupId", node.getNodeGroupId(), sql);
        sql = FormatUtils.replace("externalId", node.getExternalId(), sql);
        sql = FormatUtils.replace("nodeId", node.getNodeId(), sql);
        sql = replaceDefaultSchemaAndCatalog(sql);
        sql = FormatUtils.replace("prefixName", symmetricDialect.getTablePrefix(), sql);
        sql = FormatUtils.replace("oracleToClob",
                triggerRouter.getTrigger().isUseCaptureLobs() ? "to_clob('')||" : "", sql);

        return sql;
    }
    
    protected String castDatetimeColumnToString(String columnName) {
        return SymmetricUtils.quote(symmetricDialect, columnName);
    }

    protected String getSourceTablePrefix(Table table) {
        String prefix = (isNotBlank(table.getSchema()) ? table.getSchema() + symmetricDialect.getPlatform().getDatabaseInfo().getSchemaSeparator() : "");
        prefix = (isNotBlank(table.getCatalog()) ? table.getCatalog() + symmetricDialect.getPlatform().getDatabaseInfo().getCatalogSeparator() : "") + prefix;
        if (isBlank(prefix)) {
            prefix = (isNotBlank(symmetricDialect.getPlatform().getDefaultSchema()) ? SymmetricUtils
                    .quote(symmetricDialect, symmetricDialect.getPlatform().getDefaultSchema())
                    + "." : "");
            prefix = (isNotBlank(symmetricDialect.getPlatform().getDefaultCatalog()) ? SymmetricUtils
                    .quote(symmetricDialect, symmetricDialect.getPlatform().getDefaultCatalog())
                    + "." : "") + prefix;
        }
        return prefix;
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
            prefix = (isNotBlank(symmetricDialect.getPlatform().getDefaultCatalog()) ? SymmetricUtils
                    .quote(symmetricDialect, symmetricDialect.getPlatform().getDefaultCatalog())
                    + "." : "") + prefix;
        }
        return prefix;
    }

    protected String replaceDefaultSchemaAndCatalog(String sql) {
        String defaultCatalog = symmetricDialect.getPlatform().getDefaultCatalog();
        String defaultSchema = symmetricDialect.getPlatform().getDefaultSchema();
        sql = replaceDefaultSchema(sql, defaultSchema);
        sql = replaceDefaultCatalog(sql, defaultCatalog);
        return sql;
    }

    public String createCsvDataSql(Trigger trigger, TriggerHistory triggerHistory, Table originalTable,
            Channel channel, String whereClause) {

        Table table = originalTable.copyAndFilterColumns(triggerHistory.getParsedColumnNames(),
                triggerHistory.getParsedPkColumnNames(), true);

        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = table.getColumns();
        String columnsText = buildColumnsString(symmetricDialect.getInitialLoadTableAlias(),
                symmetricDialect.getInitialLoadTableAlias(), "", columns, DataEventType.INSERT,
                false, channel, trigger).columnString;
        sql = FormatUtils.replace("columns", columnsText, sql);
        sql = FormatUtils.replace("oracleToClob",
                trigger.isUseCaptureLobs() ? "to_clob('')||" : "", sql);

        sql = FormatUtils.replace("tableName", SymmetricUtils.quote(symmetricDialect, table.getName()), sql);
        sql = FormatUtils.replace("schemaName",
                triggerHistory == null ? getSourceTablePrefix(originalTable)
                        : getSourceTablePrefix(triggerHistory), sql);

        sql = FormatUtils.replace("whereClause", whereClause, sql);
        sql = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(symmetricDialect.getInitialLoadTableAlias(),
                        table.hasPrimaryKey() ? table.getPrimaryKeyColumns() : table.getColumns()),
                sql);

        sql = replaceDefaultSchemaAndCatalog(sql);

        return sql;
    }

    public String createCsvPrimaryKeySql(Trigger trigger, TriggerHistory triggerHistory,
            Table table, Channel channel, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = table.getPrimaryKeyColumns();
        String columnsText = buildColumnsString(symmetricDialect.getInitialLoadTableAlias(),
                symmetricDialect.getInitialLoadTableAlias(), "", columns, DataEventType.INSERT,
                false, channel, trigger).toString();
        sql = FormatUtils.replace("columns", columnsText, sql);
        sql = FormatUtils.replace("oracleToClob",
                trigger.isUseCaptureLobs() ? "to_clob('')||" : "", sql);
        sql = FormatUtils.replace("tableName", SymmetricUtils.quote(symmetricDialect, table.getName()), sql);
        sql = FormatUtils.replace("schemaName",
                triggerHistory == null ? getSourceTablePrefix(table)
                        : getSourceTablePrefix(triggerHistory), sql);
        sql = FormatUtils.replace("whereClause", whereClause, sql);
        sql = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(symmetricDialect.getInitialLoadTableAlias(),
                        table.hasPrimaryKey() ? table.getPrimaryKeyColumns() : table.getColumns()),
                sql);

        return sql;
    }

    public String createTriggerDDL(DataEventType dml, Trigger trigger, TriggerHistory history,
            Channel channel, String tablePrefix, Table originalTable, String defaultCatalog,
            String defaultSchema) {

        Table table = originalTable.copyAndFilterColumns(history.getParsedColumnNames(),
                history.getParsedPkColumnNames(), true);

		String ddl = sqlTemplates.get(dml.name().toLowerCase() + "TriggerTemplate");
    	if (dml.getDmlType().equals(DmlType.UPDATE) && trigger.isUseHandleKeyUpdates()) {
    		String temp = sqlTemplates.get(dml.name().toLowerCase() + "HandleKeyUpdates" + "TriggerTemplate");
    		if (StringUtils.trimToNull(temp)!=null) {
    			ddl=temp;
    		}
    	}
        if (ddl == null) {
            throw new NotImplementedException(dml.name() + " trigger is not implemented for "
                    + symmetricDialect.getPlatform().getName());
        }
        return replaceTemplateVariables(dml, trigger, history, channel, tablePrefix, originalTable, table,
                defaultCatalog, defaultSchema, ddl);
    }

    public String createPostTriggerDDL(DataEventType dml, Trigger trigger, TriggerHistory history,
            Channel channel, String tablePrefix, Table originalTable, String defaultCatalog,
            String defaultSchema) {

        Table table = originalTable.copyAndFilterColumns(history.getParsedColumnNames(),
                history.getParsedPkColumnNames(), true);

        String ddl = sqlTemplates.get(dml.name().toLowerCase() + "PostTriggerTemplate");
        return replaceTemplateVariables(dml, trigger, history, channel, tablePrefix, originalTable, table,
                defaultCatalog, defaultSchema, ddl);
    }

    protected String getDefaultTargetTableName(Trigger trigger, TriggerHistory history) {
        String targetTableName = null;
        if (history != null) {
            targetTableName = history.getSourceTableName();
        } else {
            targetTableName = trigger.getSourceTableName();
        }
        return targetTableName;
    }

    protected String replaceTemplateVariables(DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table originalTable, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {

        ddl = FormatUtils.replace("targetTableName", getDefaultTargetTableName(trigger, history),
                ddl);

        ddl = FormatUtils.replace("triggerName", history.getTriggerNameForDmlType(dml), ddl);
        ddl = FormatUtils.replace("channelName", trigger.getChannelId(), ddl);
        ddl = FormatUtils.replace("triggerHistoryId",
                Integer.toString(history == null ? -1 : history.getTriggerHistoryId()), ddl);
        String triggerExpression = symmetricDialect.getTransactionTriggerExpression(defaultCatalog,
                defaultSchema, trigger);
        if (symmetricDialect.isTransactionIdOverrideSupported()
                && !StringUtils.isBlank(trigger.getTxIdExpression())) {
            triggerExpression = trigger.getTxIdExpression();
        }
        ddl = FormatUtils.replace("txIdExpression",
                symmetricDialect.preProcessTriggerSqlClause(triggerExpression), ddl);

        ddl = FormatUtils.replace("channelExpression", symmetricDialect.preProcessTriggerSqlClause(getChannelExpression(trigger)),
                ddl);
        
        ddl = FormatUtils.replace("externalSelect", (trigger.getExternalSelect() == null ? "null"
                : "(" + symmetricDialect.preProcessTriggerSqlClause(trigger.getExternalSelect())
                        + ")"), ddl);

        ddl = FormatUtils.replace("syncOnInsertCondition",
                symmetricDialect.preProcessTriggerSqlClause(trigger.getSyncOnInsertCondition()),
                ddl);
        ddl = FormatUtils.replace("syncOnUpdateCondition",
                symmetricDialect.preProcessTriggerSqlClause(trigger.getSyncOnUpdateCondition()),
                ddl);
        ddl = FormatUtils.replace("syncOnDeleteCondition",
                symmetricDialect.preProcessTriggerSqlClause(trigger.getSyncOnDeleteCondition()),
                ddl);

        ddl = FormatUtils.replace("custom_on_insert_text",
                trigger.getCustomOnInsertText()==null ? "" : trigger.getCustomOnInsertText(), ddl);
        ddl = FormatUtils.replace("custom_on_update_text",
                trigger.getCustomOnUpdateText()==null ? "" : trigger.getCustomOnUpdateText(), ddl);
        ddl = FormatUtils.replace("custom_on_delete_text",
                trigger.getCustomOnDeleteText()==null ? "" : trigger.getCustomOnDeleteText(), ddl);

        ddl = FormatUtils.replace("dataHasChangedCondition", symmetricDialect
                .preProcessTriggerSqlClause(symmetricDialect.getDataHasChangedCondition(trigger)),
                ddl);
        ddl = FormatUtils.replace("sourceNodeExpression",
                symmetricDialect.getSourceNodeExpression(), ddl);

        ddl = FormatUtils.replace("oracleLobType", trigger.isUseCaptureLobs() ? "clob" : "long",
                ddl);

        String syncTriggersExpression = symmetricDialect.getSyncTriggersExpression();
        ddl = FormatUtils.replace("syncOnIncomingBatchCondition",
                trigger.isSyncOnIncomingBatch() ? Constants.ALWAYS_TRUE_CONDITION
                        : syncTriggersExpression, ddl);
        ddl = FormatUtils.replace("origTableAlias", ORIG_TABLE_ALIAS, ddl);

        Column[] orderedColumns = table.getColumns();
        ColumnString columnString = buildColumnsString(ORIG_TABLE_ALIAS, newTriggerValue,
                newColumnPrefix, orderedColumns, dml, false, channel, trigger);
        ddl = FormatUtils.replace("columns", columnString.toString(), ddl);

        ddl = replaceDefaultSchemaAndCatalog(ddl);

        ddl = FormatUtils.replace("virtualOldNewTable",
                buildVirtualTableSql(oldColumnPrefix, newColumnPrefix, originalTable.getColumns()), ddl);
        ddl = FormatUtils.replace(
                "oldColumns",
                trigger.isUseCaptureOldData() ? buildColumnsString(ORIG_TABLE_ALIAS,
                        oldTriggerValue, oldColumnPrefix, orderedColumns, dml, true, channel,
                        trigger).toString() : "null", ddl);
        ddl = eval(columnString.isBlobClob, "containsBlobClobColumns", ddl);

        // some column templates need tableName and schemaName
        ddl = FormatUtils.replace("tableName", SymmetricUtils.quote(symmetricDialect, table.getName()), ddl);
        ddl = FormatUtils.replace("schemaName", history == null ? getSourceTablePrefix(originalTable)
                : getSourceTablePrefix(history), ddl);

        Column[] primaryKeyColumns = table.getPrimaryKeyColumns();
        ddl = FormatUtils.replace(
                "oldKeys",
                buildColumnsString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix,
                        primaryKeyColumns, dml, true, channel, trigger).toString(), ddl);
        ddl = FormatUtils.replace(
                "oldNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoin(oldTriggerValue, newTriggerValue,
                        primaryKeyColumns.length == 0 ? orderedColumns : primaryKeyColumns), ddl);
        ddl = FormatUtils.replace(
                "tableNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoin(ORIG_TABLE_ALIAS, newTriggerValue,
                        primaryKeyColumns.length == 0 ? orderedColumns : primaryKeyColumns), ddl);
        ddl = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(dml == DataEventType.DELETE ? oldTriggerValue
                        : newTriggerValue, table.hasPrimaryKey() ? table.getPrimaryKeyColumns()
                        : table.getColumns()), ddl);

        String builtString = buildColumnNameString(oldTriggerValue, true, trigger,
                primaryKeyColumns);
        ddl = FormatUtils.replace("oldKeyNames", StringUtils.isNotBlank(builtString) ? ","
                + builtString : "", ddl);

        builtString = buildColumnNameString(newTriggerValue, true, trigger, primaryKeyColumns);
        ddl = FormatUtils.replace("newKeyNames", StringUtils.isNotBlank(builtString) ? ","
                + builtString : "", ddl);

        ddl = FormatUtils.replace("columnNames",
                buildColumnNameString(null, false, trigger, orderedColumns), ddl);
        ddl = FormatUtils.replace("pkColumnNames",
                buildColumnNameString(null, false, trigger, primaryKeyColumns), ddl);

        builtString = buildKeyVariablesString(primaryKeyColumns, "old");
        ddl = FormatUtils.replace("oldKeyVariables", StringUtils.isNotBlank(builtString) ? ","
                + builtString : "", ddl);

        builtString = buildKeyVariablesString(primaryKeyColumns, "new");
        ddl = FormatUtils.replace("newKeyVariables", StringUtils.isNotBlank(builtString) ? ","
                + builtString : "", ddl);

        ddl = FormatUtils.replace("varNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoinVar(newTriggerValue, "new", primaryKeyColumns), ddl);
        ddl = FormatUtils.replace("varOldPrimaryKeyJoin",
                aliasedPrimaryKeyJoinVar(oldTriggerValue, "old", primaryKeyColumns), ddl);

        // replace $(newTriggerValue) and $(oldTriggerValue)
        ddl = FormatUtils.replace("newTriggerValue", newTriggerValue, ddl);
        ddl = FormatUtils.replace("oldTriggerValue", oldTriggerValue, ddl);
        ddl = FormatUtils.replace("newColumnPrefix", newColumnPrefix, ddl);
        ddl = FormatUtils.replace("oldColumnPrefix", oldColumnPrefix, ddl);
        ddl = FormatUtils.replace("prefixName", tablePrefix, ddl);
        ddl = replaceDefaultSchemaAndCatalog(ddl);

        ddl = FormatUtils.replace("oracleToClob",
                trigger.isUseCaptureLobs() ? "to_clob('')||" : "", ddl);

        switch (dml) {
            case DELETE:
                ddl = FormatUtils.replace("curTriggerValue", oldTriggerValue, ddl);
                ddl = FormatUtils.replace("curColumnPrefix", oldColumnPrefix, ddl);
                break;
            case INSERT:
            case UPDATE:
            default:
                ddl = FormatUtils.replace("curTriggerValue", newTriggerValue, ddl);
                ddl = FormatUtils.replace("curColumnPrefix", newColumnPrefix, ddl);
                break;
        }
        return ddl;
    }
    
    protected String getChannelExpression(Trigger trigger) {
        if (trigger.getChannelId().equals(Constants.CHANNEL_DYNAMIC)) {
            if (StringUtils.isNotBlank(trigger.getChannelExpression())) {
                return trigger.getChannelExpression();
            } else {
                throw new IllegalStateException("When the channel is set to '" + Constants.CHANNEL_DYNAMIC + "', a channel expression must be provided.");
            }
        } else {
            return "'" + trigger.getChannelId() + "'";
        }
    }

    protected String buildVirtualTableSql(String oldTriggerValue, String newTriggerValue,
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

    protected String eval(boolean condition, String prop, String ddl) {
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
                        ddl = ddl.substring(0, ifIndex) + onTrue
                                + ddl.substring(endIndex + endStmt.length());
                    } else {
                        ddl = ddl.substring(0, ifIndex) + onFalse
                                + ddl.substring(endIndex + endStmt.length());
                    }

                } else {
                    throw new IllegalStateException(ifStmt + " has to have a " + endStmt);
                }
            }
        }
        return ddl;
    }

    protected String aliasedPrimaryKeyJoin(String aliasOne, String aliasTwo, Column[] columns) {
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

    protected String aliasedPrimaryKeyJoinVar(String alias, String prefix, Column[] columns) {
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

    /**
     * Specific to Derby. Needs to be removed when the initial load is
     * refactored to concat in Java vs. in SQL
     */
    @Deprecated
    protected String getPrimaryKeyWhereString(String alias, Column[] columns) {
        return null;
    }

    protected boolean requiresWrappedBlobTemplateForBlobType() {
        return false;
    }

    protected boolean requiresEmptyLobTemplateForDeletes() {
        return false;
    }

    protected String buildColumnNameString(String tableAlias, boolean quote, Trigger trigger,
            Column[] columns) {
        StringBuilder columnsText = new StringBuilder();
        for (Column column : columns) {
            boolean isLob = symmetricDialect.getPlatform().isLob(column.getMappedTypeCode());
            String columnName = column.getName();
            if (quote) {
                columnName = SymmetricUtils.quote(symmetricDialect, columnName);
            }

            if (!(isLob && trigger.isUseStreamLobs())) {
                if (StringUtils.isNotBlank(tableAlias)) {
                    columnsText.append(tableAlias);
                    columnsText.append(".");
                }
                columnsText.append(columnName);
            } else {
            	columnsText.append("null");            	
            }
            columnsText.append(",");
        }
        
        return columnsText.length() > 0 ? columnsText.substring(0, columnsText.length() - 1) : columnsText.toString();
    }

    protected ColumnString buildColumnsString(String origTableAlias, String tableAlias,
            String columnPrefix, Column[] columns, DataEventType dml, boolean isOld,
            Channel channel, Trigger trigger) {
        String columnsText = "";
        boolean containsLob = false;

        String lastCommandToken = symmetricDialect.escapesTemplatesForDatabaseInserts() ? (triggerConcatCharacter
                + "'',''" + triggerConcatCharacter)
                : (triggerConcatCharacter + "','" + triggerConcatCharacter);

        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (column != null) {
                ColumnString columnString = fillOutColumnTemplate(origTableAlias, tableAlias,
                        columnPrefix, column, dml, isOld, channel, trigger);
                columnsText = columnsText + "\n          " + columnString.columnString
                        + lastCommandToken;
                containsLob |= columnString.isBlobClob;
            }

        }

        if (columnsText.endsWith(lastCommandToken)) {
            columnsText = columnsText
                    .substring(0, columnsText.length() - lastCommandToken.length());
        }

        return new ColumnString(columnsText, containsLob);
    }
    
    protected ColumnString fillOutColumnTemplate(String origTableAlias, String tableAlias,
            String columnPrefix, Column column, DataEventType dml, boolean isOld, Channel channel,
            Trigger trigger) {
        boolean isLob = symmetricDialect.getPlatform().isLob(column.getMappedTypeCode());
        String templateToUse = null;
        if (column.getJdbcTypeName() != null
                && (column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOMETRY))
                && StringUtils.isNotBlank(geometryColumnTemplate)) {
            templateToUse = geometryColumnTemplate;
        } 
        else if (column.getJdbcTypeName() != null
                && (column.getJdbcTypeName().toUpperCase().contains(TypeMap.GEOGRAPHY))
                && StringUtils.isNotBlank(geographyColumnTemplate)) {
            templateToUse = geographyColumnTemplate;
        } 
        else {
            switch (column.getMappedTypeCode()) {
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
                case Types.NCHAR:
                case Types.VARCHAR:
                case ColumnTypes.NVARCHAR:
                    templateToUse = stringColumnTemplate;
                    break;
                case ColumnTypes.SQLXML:
                    templateToUse = xmlColumnTemplate;
                    break;
                case Types.ARRAY:
                    templateToUse = arrayColumnTemplate;
                    break;
                case Types.LONGVARCHAR:
                case ColumnTypes.LONGNVARCHAR:
                    if (!isLob) {
                        templateToUse = stringColumnTemplate;
                        break;
                    }
                case Types.CLOB:
                    if (isOld && symmetricDialect.needsToSelectLobData()) {
                        templateToUse = emptyColumnTemplate;
                    } else {
                        templateToUse = clobColumnTemplate;
                    }
                    break;
                case Types.BINARY:
                case Types.VARBINARY:
                    if (isNotBlank(binaryColumnTemplate)) {
                        templateToUse = binaryColumnTemplate;
                        break;
                    }
                case Types.BLOB:
                    if (requiresWrappedBlobTemplateForBlobType()) {
                        templateToUse = wrappedBlobColumnTemplate;
                        break;
                    }                    
                case Types.LONGVARBINARY:                   
                case -10:  // SQL-Server ntext binary type
                    if (column.getJdbcTypeName() != null
                            && (column.getJdbcTypeName().toUpperCase().contains(TypeMap.IMAGE))
                            && StringUtils.isNotBlank(imageColumnTemplate)) {
                        if (isOld) {
                            templateToUse = emptyColumnTemplate;
                        } else {
                            templateToUse = imageColumnTemplate;
                        }
                    } else if (isOld && symmetricDialect.needsToSelectLobData()) {
                        templateToUse = emptyColumnTemplate;
                    } else {
                        templateToUse = blobColumnTemplate;
                    }
                    break;
                case Types.DATE:
                    if (noDateColumnTemplate()) {
                        templateToUse = datetimeColumnTemplate;
                        break;
                    }
                    templateToUse = dateColumnTemplate;
                    break;
                case Types.TIME:
                    if (noTimeColumnTemplate()) {
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
                default:
                    if (column.getJdbcTypeName() != null) {
                        if (column.getJdbcTypeName().toUpperCase().equals(TypeMap.INTERVAL)) {
                            templateToUse = numberColumnTemplate;
                            break;
                        } else if (column.getMappedType().equals(TypeMap.TIMESTAMPTZ)
                                && StringUtils.isNotBlank(this.dateTimeWithTimeZoneColumnTemplate)) {
                            templateToUse = this.dateTimeWithTimeZoneColumnTemplate;
                            break;
                        } else if (column.getMappedType().equals(TypeMap.TIMESTAMPLTZ)
                                && StringUtils
                                        .isNotBlank(this.dateTimeWithLocalTimeZoneColumnTemplate)) {
                            templateToUse = this.dateTimeWithLocalTimeZoneColumnTemplate;
                            break;
                        }

                    }

                    if (StringUtils.isBlank(templateToUse)
                            && StringUtils.isNotBlank(this.otherColumnTemplate)) {
                        templateToUse = this.otherColumnTemplate;
                        break;
                    }

                    throw new NotImplementedException(column.getName() + " is of type "
                            + column.getMappedType() + " with JDBC type of "
                            + column.getJdbcTypeName());
            }
        }

        if (dml == DataEventType.DELETE && isLob && requiresEmptyLobTemplateForDeletes()) {
            templateToUse = emptyColumnTemplate;
        } else if (isLob && trigger.isUseStreamLobs()) {
            templateToUse = emptyColumnTemplate;
        }

        if (templateToUse != null) {
            templateToUse = templateToUse.trim();
        } else {
            throw new NotImplementedException();
        }

        String formattedColumnText = FormatUtils.replace("columnName",
                String.format("%s%s", columnPrefix, column.getName()), templateToUse);
        
        formattedColumnText = FormatUtils.replace("columnSize",
                getColumnSize(column), formattedColumnText);

        formattedColumnText = FormatUtils.replace("masterCollation",
                symmetricDialect.getMasterCollation(), formattedColumnText);

        if (isLob) {
            formattedColumnText = symmetricDialect.massageForLob(formattedColumnText, channel);
        }

        formattedColumnText = FormatUtils.replace("origTableAlias", origTableAlias,
                formattedColumnText);
        formattedColumnText = FormatUtils.replace("tableAlias", tableAlias, formattedColumnText);
        formattedColumnText = FormatUtils.replace("prefixName", symmetricDialect.getTablePrefix(),
                formattedColumnText);

        return new ColumnString(formattedColumnText, isLob);

    }
    
    protected String getColumnSize(Column column) {
        return column.getSize();
    }

    public String getOtherColumnTemplate() {
        return otherColumnTemplate;
    }

    protected boolean noTimeColumnTemplate() {
        return timeColumnTemplate == null || timeColumnTemplate.equals("null")
                || timeColumnTemplate.trim().equals("");
    }

    protected boolean noDateColumnTemplate() {
        return dateColumnTemplate == null || dateColumnTemplate.equals("null")
                || dateColumnTemplate.trim().equals("");
    }

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
                case ColumnTypes.NVARCHAR:
                case ColumnTypes.LONGNVARCHAR:
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

    protected String buildKeyVariablesString(Column[] columns, String prefix) {
        String text = "";
        for (int i = 0; i < columns.length; i++) {
            text += "@" + prefix + "pk" + i;
            if (i + 1 < columns.length) {
                text += ", ";
            }
        }
        return text;
    }

    public String getClobColumnTemplate() {
        return clobColumnTemplate;
    }

    public void setBooleanColumnTemplate(String booleanColumnTemplate) {
        this.booleanColumnTemplate = booleanColumnTemplate;
    }

    public String getNewTriggerValue() {
        return newTriggerValue;
    }

    public String getOldTriggerValue() {
        return oldTriggerValue;
    }

    public String getBlobColumnTemplate() {
        return blobColumnTemplate;
    }

    public String getWrappedBlobColumnTemplate() {
        return wrappedBlobColumnTemplate;
    }

    protected String replaceDefaultSchema(String ddl, String defaultSchema) {
        if (StringUtils.isNotBlank(defaultSchema)) {
            ddl = FormatUtils.replace("defaultSchema", SymmetricUtils.quote(symmetricDialect, defaultSchema) + ".", ddl);
        } else {
            ddl = FormatUtils.replace("defaultSchema", "", ddl);
        }
        return ddl;
    }

    protected String replaceDefaultCatalog(String ddl, String defaultCatalog) {
        if (StringUtils.isNotBlank(defaultCatalog)) {
            ddl = FormatUtils.replace("defaultCatalog", SymmetricUtils.quote(symmetricDialect, defaultCatalog) + ".", ddl);
        } else {
            ddl = FormatUtils.replace("defaultCatalog", "", ddl);
        }
        return ddl;
    }

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

    public String getImageColumnTemplate() {
        return imageColumnTemplate;
    }

    public void setImageColumnTemplate(String imageColumnTemplate) {
        this.imageColumnTemplate = imageColumnTemplate;
    }

    protected class ColumnString {

        String columnString;
        boolean isBlobClob = false;

        ColumnString(String columnExpression, boolean isBlobClob) {
            this.columnString = columnExpression;
            this.isBlobClob = isBlobClob;
        }

        @Override
        public String toString() {
            return StringUtils.isBlank(columnString) ? "null" : columnString;
        }

    }

    public int toHashedValue() {
        int hashedValue = 0;
        if (sqlTemplates != null) {
            for (String key : sqlTemplates.keySet()) {
                hashedValue += sqlTemplates.get(key).hashCode();
            }
            
            Field[] fields = getClass().getSuperclass().getDeclaredFields();
            for (Field field : fields) {
                if (field.getType().equals(String.class)) {
                    try {
                        String value = (String)field.get(this);
                        if (value != null) {
                            hashedValue += value.hashCode();
                        }
                    } catch (Exception e) {
                        log.warn("Failed to get hash code for field " + field.getName());
                    }
                }
            }
        }
        return hashedValue;
    }
}
