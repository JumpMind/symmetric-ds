/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import java.sql.Types;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.apache.commons.lang.StringUtils;
import org.jumpmind.db.model.Column;
import org.jumpmind.db.model.Table;
import org.jumpmind.symmetric.Version;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.mssql.MsSqlDbDialect;
import org.jumpmind.symmetric.db.postgresql.PostgreSqlDbDialect;
import org.jumpmind.symmetric.io.data.DataEventType;
import org.jumpmind.symmetric.model.Channel;
import org.jumpmind.symmetric.model.Node;
import org.jumpmind.symmetric.model.Trigger;
import org.jumpmind.symmetric.model.TriggerHistory;
import org.jumpmind.symmetric.model.TriggerRouter;
import org.jumpmind.util.FormatUtils;

/**
 * Responsible for generating dialect specific SQL such as trigger bodies and
 * functions
 */
public class SqlTemplate {

    private static final String ORIG_TABLE_ALIAS = "orig";

    static final String INSERT_TRIGGER_TEMPLATE = "insertTriggerTemplate";

    static final String UPDATE_TRIGGER_TEMPLATE = "updateTriggerTemplate";

    static final String DELETE_TRIGGER_TEMPLATE = "deleteTriggerTemplate";

    static final String INITIAL_LOAD_SQL_TEMPLATE = "initialLoadSqlTemplate";

    private Map<String, String> sqlTemplates;

    private Map<String, String> functionTemplatesToInstall;

    private String functionInstalledSql;

    private String emptyColumnTemplate = "''";

    private String stringColumnTemplate;

    private String xmlColumnTemplate;

    private String arrayColumnTemplate;

    private String numberColumnTemplate;

    private String datetimeColumnTemplate;

    private String timeColumnTemplate;

    private String dateColumnTemplate;

    private String clobColumnTemplate;

    private String blobColumnTemplate;

    private String wrappedBlobColumnTemplate;

    private String booleanColumnTemplate;

    private String triggerConcatCharacter;

    private String newTriggerValue;

    private String oldTriggerValue;

    private String oldColumnPrefix = "";

    private String newColumnPrefix = "";

    private String otherColumnTemplate;
    
    public String createInitalLoadSql(Node node, IDbDialect dialect, TriggerRouter triggerRouter,
            Table table, TriggerHistory triggerHistory, Channel channel) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);
        Column[] columns = dialect.orderColumns(triggerHistory.getParsedColumnNames(), table);
        String columnsText = buildColumnString(dialect.getInitialLoadTableAlias(),
                dialect.getInitialLoadTableAlias(), "", columns, dialect, DataEventType.INSERT,
                false, channel, triggerRouter.getTrigger()).columnString;

        sql = FormatUtils.replace("columns", columnsText, sql);
        sql = FormatUtils
                .replace(
                        "whereClause",
                        StringUtils.isBlank(triggerRouter.getInitialLoadSelect()) ? Constants.ALWAYS_TRUE_CONDITION
                                : triggerRouter.getInitialLoadSelect(), sql);       
        sql = FormatUtils.replace("tableName", quote(table.getName(), dialect), sql);
        sql = FormatUtils.replace("schemaName", triggerHistory == null ? getSourceTablePrefix(triggerRouter.getTrigger(), dialect) : getSourceTablePrefix(triggerHistory, dialect), sql);
        sql = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(dialect.getInitialLoadTableAlias(),
                        table.getPrimaryKeyColumns()), sql);

        // Replace these parameters to give the initiaLoadContition a chance to
        // reference the node that is being loaded
        sql = FormatUtils.replace("groupId", node.getNodeGroupId(), sql);
        sql = FormatUtils.replace("externalId", node.getExternalId(), sql);
        sql = FormatUtils.replace("nodeId", node.getNodeId(), sql);
        sql = replaceDefaultSchemaAndCatalog(dialect, triggerRouter.getTrigger(), sql);
        sql = FormatUtils.replace("prefixName", dialect.getTablePrefix(), sql);
        sql = FormatUtils.replace("oracleToClob",
                triggerRouter.getTrigger().isUseCaptureLobs() ? "to_clob('')||" : "", sql);

        return sql;
    }

    protected String getSourceTablePrefix(Trigger trigger, IDbDialect dbDialect) {
        String schemaPlus = (trigger.getSourceSchemaName() != null ?
                trigger.getSourceSchemaName() + "." : "");
        String catalogPlus = (trigger.getSourceCatalogName() != null ?
                trigger.getSourceCatalogName() + "." : "")
                + schemaPlus;
        return catalogPlus;
    }
    
    protected String getSourceTablePrefix(TriggerHistory triggerHistory, IDbDialect dbDialect) {
        String schemaPlus = (triggerHistory.getSourceSchemaName() != null ? 
                triggerHistory.getSourceSchemaName() + "." : "");
        String catalogPlus = (triggerHistory.getSourceCatalogName() != null ? 
                triggerHistory.getSourceCatalogName() + "." : "")
                + schemaPlus;
        return catalogPlus;
    }    

    protected String quote(String name, IDbDialect dbDialect) {
        String quote = dbDialect.getPlatform().getPlatformInfo().getIdentifierQuoteString();
        if (StringUtils.isNotBlank(quote)) {
            return quote + name + quote;
        } else {
            return name;
        }
    }

    protected String replaceDefaultSchemaAndCatalog(IDbDialect dbDialect, Trigger trigger,
            String sql) {        
        String defaultCatalog = dbDialect.getPlatform().getDefaultCatalog();
        String defaultSchema = dbDialect.getPlatform().getDefaultSchema();

        boolean resolveSchemaAndCatalogs = trigger.getSourceCatalogName() != null
        || trigger.getSourceSchemaName() != null;
        
        sql = FormatUtils.replace("defaultSchema", resolveSchemaAndCatalogs && defaultSchema != null
                && defaultSchema.length() > 0 ? defaultSchema + "." : "", sql);

        return FormatUtils.replace("defaultCatalog", resolveSchemaAndCatalogs
                && defaultCatalog != null && defaultCatalog.length() > 0 ? defaultCatalog + "."
                : "", sql);
    }

    public String createCsvDataSql(IDbDialect dialect, Trigger trigger, TriggerHistory triggerHistory, Table table,
            Channel channel, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = trigger.orderColumnsForTable(table);
        String columnsText = buildColumnString(dialect.getInitialLoadTableAlias(),
                dialect.getInitialLoadTableAlias(), "", columns, dialect, DataEventType.INSERT,
                false, channel, trigger).columnString;
        sql = FormatUtils.replace("columns", columnsText, sql);
        sql = FormatUtils.replace("oracleToClob", trigger.isUseCaptureLobs() ? "to_clob('')||" : "",
                sql);

        sql = FormatUtils.replace("tableName", quote(table.getName(), dialect), sql);
        sql = FormatUtils.replace("schemaName", triggerHistory == null ? getSourceTablePrefix(trigger, dialect) : getSourceTablePrefix(triggerHistory, dialect), sql);

        sql = FormatUtils.replace("whereClause", whereClause, sql);
        sql = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(dialect.getInitialLoadTableAlias(),
                        table.getPrimaryKeyColumns()), sql);

        sql = replaceDefaultSchemaAndCatalog(dialect, trigger, sql);

        return sql;
    }

    public String createCsvPrimaryKeySql(IDbDialect dialect, Trigger trigger, TriggerHistory triggerHistory, Table table,
            Channel channel, String whereClause) {
        String sql = sqlTemplates.get(INITIAL_LOAD_SQL_TEMPLATE);

        Column[] columns = table.getPrimaryKeyColumns();
        String columnsText = buildColumnString(dialect.getInitialLoadTableAlias(),
                dialect.getInitialLoadTableAlias(), "", columns, dialect, DataEventType.INSERT,
                false, channel, trigger).toString();
        sql = FormatUtils.replace("columns", columnsText, sql);
        sql = FormatUtils.replace("oracleToClob", trigger.isUseCaptureLobs() ? "to_clob('')||" : "",
                sql);
        sql = FormatUtils.replace("tableName", quote(table.getName(), dialect), sql);
        sql = FormatUtils.replace("schemaName", triggerHistory == null ? getSourceTablePrefix(trigger, dialect) : getSourceTablePrefix(triggerHistory, dialect), sql);
        sql = FormatUtils.replace("whereClause", whereClause, sql);
        sql = FormatUtils.replace("primaryKeyWhereString",
                getPrimaryKeyWhereString(dialect.getInitialLoadTableAlias(), columns), sql);

        return sql;
    }

    public String[] getFunctionsToInstall() {
        if (functionTemplatesToInstall != null) {
            return functionTemplatesToInstall.keySet().toArray(
                    new String[functionTemplatesToInstall.size()]);
        } else {
            return new String[0];
        }
    }

    public String createTriggerDDL(IDbDialect dialect, DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table,
            String defaultCatalog, String defaultSchema) {
        String ddl = sqlTemplates.get(dml.name().toLowerCase() + "TriggerTemplate");
        if (ddl == null) {
            throw new NotImplementedException(dml.name() + " trigger is not implemented for "
                    + dialect.getPlatform().getName());
        }
        return replaceTemplateVariables(dialect, dml, trigger, history, channel, tablePrefix,
                table, defaultCatalog, defaultSchema, ddl);
    }

    public String createPostTriggerDDL(IDbDialect dialect, DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table,
            String defaultCatalog, String defaultSchema) {
        String ddl = sqlTemplates.get(dml.name().toLowerCase() + "PostTriggerTemplate");
        return replaceTemplateVariables(dialect, dml, trigger, history, channel, tablePrefix,
                table, defaultCatalog, defaultSchema, ddl);
    }

    private String getDefaultTargetTableName(Trigger trigger, TriggerHistory history) {
        String targetTableName = null;
        if (history != null) {
            targetTableName = history.getSourceTableName();
        } else {
            targetTableName = trigger.getSourceTableName();
        }
        return targetTableName;
    }

    public String replaceTemplateVariables(IDbDialect dialect, DataEventType dml, Trigger trigger,
            TriggerHistory history, Channel channel, String tablePrefix, Table table,
            String defaultCatalog, String defaultSchema, String ddl) {

        ddl = FormatUtils.replace("targetTableName", getDefaultTargetTableName(trigger, history), ddl);

        ddl = FormatUtils.replace("triggerName", history.getTriggerNameForDmlType(dml), ddl);
        ddl = FormatUtils.replace("channelName", trigger.getChannelId(), ddl);
        ddl = FormatUtils.replace("triggerHistoryId",
                Integer.toString(history == null ? -1 : history.getTriggerHistoryId()), ddl);
        String triggerExpression = dialect.getTransactionTriggerExpression(defaultCatalog,
                defaultSchema, trigger);
        if (dialect.isTransactionIdOverrideSupported()
                && !StringUtils.isBlank(trigger.getTxIdExpression())) {
            triggerExpression = trigger.getTxIdExpression();
        }
        ddl = FormatUtils.replace("txIdExpression",
                dialect.preProcessTriggerSqlClause(triggerExpression), ddl);

        ddl = FormatUtils
                .replace("externalSelect", (trigger.getExternalSelect() == null ? "null" : "("
                        + dialect.preProcessTriggerSqlClause(trigger.getExternalSelect()) + ")"),
                        ddl);

        ddl = FormatUtils.replace("syncOnInsertCondition",
                dialect.preProcessTriggerSqlClause(trigger.getSyncOnInsertCondition()), ddl);
        ddl = FormatUtils.replace("syncOnUpdateCondition",
                dialect.preProcessTriggerSqlClause(trigger.getSyncOnUpdateCondition()), ddl);
        ddl = FormatUtils.replace("syncOnDeleteCondition",
                dialect.preProcessTriggerSqlClause(trigger.getSyncOnDeleteCondition()), ddl);
        ddl = FormatUtils.replace("dataHasChangedCondition",
                dialect.preProcessTriggerSqlClause(dialect.getDataHasChangedCondition(trigger)),
                ddl);
        ddl = FormatUtils.replace("sourceNodeExpression", dialect.getSourceNodeExpression(), ddl);

        ddl = FormatUtils.replace("oracleLobType", trigger.isUseCaptureLobs() ? "clob" : "long", ddl);

        String syncTriggersExpression = dialect.getSyncTriggersExpression();
        ddl = FormatUtils.replace("syncOnIncomingBatchCondition",
                trigger.isSyncOnIncomingBatch() ? Constants.ALWAYS_TRUE_CONDITION
                        : syncTriggersExpression, ddl);
        ddl = FormatUtils.replace("origTableAlias", ORIG_TABLE_ALIAS, ddl);

        Column[] columns = trigger.orderColumnsForTable(table);
        ColumnString columnString = buildColumnString(ORIG_TABLE_ALIAS, newTriggerValue,
                newColumnPrefix, columns, dialect, dml, false, channel, trigger);
        ddl = FormatUtils.replace("columns", columnString.toString(), ddl);

        ddl = replaceDefaultSchemaAndCatalog(dialect, trigger, ddl);

        ddl = FormatUtils
                .replace(
                        "virtualOldNewTable",
                        buildVirtualTableSql(dialect, oldColumnPrefix, newColumnPrefix,
                                table.getColumns()), ddl);
        ddl = FormatUtils.replace(
                "oldColumns",
                buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix, columns,
                        dialect, dml, true, channel, trigger).toString(), ddl);
        ddl = eval(columnString.isBlobClob, "containsBlobClobColumns", ddl);
        
        // some column templates need tableName and schemaName
        ddl = FormatUtils.replace("tableName", history == null ? quote(trigger.getSourceTableName(), dialect)
                : quote(history.getSourceTableName(), dialect), ddl);
        ddl = FormatUtils.replace("schemaName",
                history == null ? getSourceTablePrefix(trigger, dialect) : getSourceTablePrefix(history, dialect), ddl);

        columns = table.getPrimaryKeyColumns();
        ddl = FormatUtils.replace(
                "oldKeys",
                buildColumnString(ORIG_TABLE_ALIAS, oldTriggerValue, oldColumnPrefix, columns,
                        dialect, dml, true, channel, trigger).toString(), ddl);
        ddl = FormatUtils.replace("oldNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoin(oldTriggerValue, newTriggerValue, columns.length == 0 ? table.getColumns() : columns), ddl);
        ddl = FormatUtils.replace("tableNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoin(ORIG_TABLE_ALIAS, newTriggerValue, columns.length == 0 ? table.getColumns() : columns), ddl);
        ddl = FormatUtils.replace(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(dml == DataEventType.DELETE ? oldTriggerValue
                        : newTriggerValue, columns), ddl);

        ddl = FormatUtils.replace("declareOldKeyVariables", buildKeyVariablesDeclare(columns, "old"),
                ddl);
        ddl = FormatUtils.replace("declareNewKeyVariables", buildKeyVariablesDeclare(columns, "new"),
                ddl);
        
        String builtString = buildColumnNameString(oldTriggerValue, columns);
        ddl = FormatUtils.replace("oldKeyNames", StringUtils.isNotBlank(builtString) ? ","+builtString:"", ddl);
        
        builtString = buildColumnNameString(newTriggerValue, columns);
        ddl = FormatUtils.replace("newKeyNames", StringUtils.isNotBlank(builtString) ? ","+builtString:"", ddl);
        
        builtString = buildKeyVariablesString(columns, "old");
        ddl = FormatUtils.replace("oldKeyVariables", StringUtils.isNotBlank(builtString) ? ","+builtString:"", ddl);
        
        builtString = buildKeyVariablesString(columns, "new");
        ddl = FormatUtils.replace("newKeyVariables", StringUtils.isNotBlank(builtString) ? ","+builtString:"", ddl);
        
        ddl = FormatUtils.replace("varNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoinVar(newTriggerValue, "new", columns), ddl);
        ddl = FormatUtils.replace("varOldPrimaryKeyJoin",
                aliasedPrimaryKeyJoinVar(oldTriggerValue, "old", columns), ddl);

        // replace $(newTriggerValue) and $(oldTriggerValue)
        ddl = FormatUtils.replace("newTriggerValue", newTriggerValue, ddl);
        ddl = FormatUtils.replace("oldTriggerValue", oldTriggerValue, ddl);
        ddl = FormatUtils.replace("newColumnPrefix", newColumnPrefix, ddl);
        ddl = FormatUtils.replace("oldColumnPrefix", oldColumnPrefix, ddl);
        ddl = FormatUtils.replace("prefixName", tablePrefix, ddl);
        ddl = replaceDefaultSchemaAndCatalog(dialect, trigger, ddl);
        
        ddl = FormatUtils.replace("oracleToClob", trigger.isUseCaptureLobs() ? "to_clob('')||" : "",
                ddl);

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

    private String buildVirtualTableSql(IDbDialect dialect, String oldTriggerValue,
            String newTriggerValue, Column[] columns) {
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
        StringBuilder b = new StringBuilder("'");
        for (Column column : columns) {
            b.append("\"").append(column.getName()).append("\"=");
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
            if (!column.equals(columns[columns.length - 1])) {
                b.append(" and ");
            }
        }
        b.append("'");
        return b.toString();
    }

    private ColumnString buildColumnString(String origTableAlias, String tableAlias,
            String columnPrefix, Column[] columns, IDbDialect dbDialect, DataEventType dml,
            boolean isOld, Channel channel, Trigger trigger) {
        String columnsText = "";
        boolean isLob = false;

        String lastCommandToken = dbDialect.escapesTemplatesForDatabaseInserts() ? (triggerConcatCharacter
                + "'',''" + triggerConcatCharacter)
                : (triggerConcatCharacter + "','" + triggerConcatCharacter);

        for (int i = 0; i < columns.length; i++) {
            Column column = columns[i];
            if (column != null) {
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
                    templateToUse = stringColumnTemplate;
                    break;
                case Types.LONGVARCHAR:
                    templateToUse = stringColumnTemplate;
                    isLob = true;
                    break;
                case Types.SQLXML:
                    templateToUse = xmlColumnTemplate;
                    break;
                case Types.ARRAY:
                    templateToUse = arrayColumnTemplate;
                    break;
                case Types.CLOB:
                    if (isOld && dbDialect.needsToSelectLobData()) {
                        templateToUse = emptyColumnTemplate;
                    } else {
                        templateToUse = clobColumnTemplate;
                    }
                    isLob = true;
                    break;
                case Types.BLOB:
                    if (dbDialect instanceof PostgreSqlDbDialect) {
                        templateToUse = wrappedBlobColumnTemplate;
                        isLob = true;
                        break;
                    }
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    // SQL-Server ntext binary type
                case -10:
                    if (isOld && dbDialect.needsToSelectLobData()) {
                        templateToUse = emptyColumnTemplate;
                    } else {
                        templateToUse = blobColumnTemplate;
                    }
                    isLob = true;
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
                case Types.NULL:
                case Types.OTHER:
                	templateToUse = this.otherColumnTemplate;
                	break;
                case Types.JAVA_OBJECT:
                case Types.DISTINCT:
                case Types.STRUCT:
                case Types.REF:
                case Types.DATALINK:
                default:
                    if (column.getJdbcTypeName() != null
                            && column.getJdbcTypeName().equalsIgnoreCase("interval")) {
                        templateToUse = numberColumnTemplate;
                        break;
                    }
                    throw new NotImplementedException(column.getName() + " is of type "
                            + column.getType() + " with JDBC type of " + column.getJdbcTypeName());
                }

                if (dml == DataEventType.DELETE && isLob && dbDialect instanceof MsSqlDbDialect) {
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

                formattedColumnText = FormatUtils.replace("masterCollation",
                        dbDialect.getMasterCollation(), formattedColumnText);

                if (isLob) {
                    formattedColumnText = dbDialect.massageForLob(formattedColumnText, channel);
                }

                columnsText = columnsText + "\n          " + formattedColumnText + lastCommandToken;
            }

        }

        if (columnsText.endsWith(lastCommandToken)) {
            columnsText = columnsText
                    .substring(0, columnsText.length() - lastCommandToken.length());
        }

        columnsText = FormatUtils.replace("origTableAlias", origTableAlias, columnsText);
        columnsText = FormatUtils.replace("tableAlias", tableAlias, columnsText);
        columnsText = FormatUtils.replace("prefixName", dbDialect.getTablePrefix(), columnsText);
        return new ColumnString(columnsText, isLob);
    }

    public String getOtherColumnTemplate() {
		return otherColumnTemplate;
	}

	public void setOtherColumnTemplate(String otherColumnTemplate) {
		this.otherColumnTemplate = otherColumnTemplate;
	}

	private boolean noTimeColumnTemplate() {
        return timeColumnTemplate == null || timeColumnTemplate.equals("null")
                || timeColumnTemplate.trim().equals("");
    }

    private boolean noDateColumnTemplate() {
        return dateColumnTemplate == null || dateColumnTemplate.equals("null")
                || dateColumnTemplate.trim().equals("");
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
        		text +="varbinary(max)\n";
        		break;
            default:
                if (columns[i].getJdbcTypeName() != null
                        && columns[i].getJdbcTypeName().equalsIgnoreCase("interval")) {
                    text += "interval";
                    break;
                }
                throw new NotImplementedException(columns[i] + " is of type "
                        + columns[i].getType());
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

    public void setStringColumnTemplate(String columnTemplate) {
        this.stringColumnTemplate = columnTemplate;
    }

    public void setXmlColumnTemplate(String columnTemplate) {
        this.xmlColumnTemplate = columnTemplate;
    }

    public void setArrayColumnTemplate(String arrayColumnTemplate) {
        this.arrayColumnTemplate = arrayColumnTemplate;
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

    public String getBlobColumnTemplate() {
        return blobColumnTemplate;
    }

    public void setBlobColumnTemplate(String blobColumnTemplate) {
        this.blobColumnTemplate = blobColumnTemplate;
    }

    public String getWrappedBlobColumnTemplate() {
        return wrappedBlobColumnTemplate;
    }

    public void setWrappedBlobColumnTemplate(String wrappedBlobColumnTemplate) {
        this.wrappedBlobColumnTemplate = wrappedBlobColumnTemplate;
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

    public void setEmptyColumnTemplate(String emptyColumnTemplate) {
        this.emptyColumnTemplate = emptyColumnTemplate;
    }

    public void setNewColumnPrefix(String newColumnPrefix) {
        this.newColumnPrefix = newColumnPrefix;
    }

    public String getFunctionSql(String functionKey, String functionName, String defaultSchema) {
        if (this.functionTemplatesToInstall != null) {
            String ddl = FormatUtils.replace("functionName", functionName,
                    this.functionTemplatesToInstall.get(functionKey));
            ddl = FormatUtils.replace("version", Version.versionWithUnderscores(), ddl);
            ddl = FormatUtils.replace("defaultSchema", defaultSchema != null
                    && defaultSchema.length() > 0 ? defaultSchema + "." : "", ddl);
            return ddl;
        } else {
            return null;
        }
    }

    public String getFunctionInstalledSql(String functionName, String defaultSchema) {
        if (functionInstalledSql != null) {
            String ddl = FormatUtils.replace("functionName", functionName, functionInstalledSql);
            ddl = FormatUtils.replace("version", Version.versionWithUnderscores(), ddl);
            ddl = FormatUtils.replace("defaultSchema", defaultSchema != null
                    && defaultSchema.length() > 0 ? defaultSchema : "", ddl);
            return ddl;
        } else {
            return null;
        }
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

    private class ColumnString {

        String columnString;
        boolean isBlobClob = false;

        ColumnString(String columnExpression, boolean isBlobClob) {
            this.columnString = columnExpression;
            this.isBlobClob = isBlobClob;
        }

        public String toString() {
            return StringUtils.isBlank(columnString) ? "null" : columnString;
        }

    }
}