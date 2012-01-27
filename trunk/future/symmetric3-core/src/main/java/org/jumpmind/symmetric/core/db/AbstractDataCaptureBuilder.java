package org.jumpmind.symmetric.core.db;

import java.sql.Types;
import java.util.Map;

import org.jumpmind.symmetric.core.Version;
import org.jumpmind.symmetric.core.common.Log;
import org.jumpmind.symmetric.core.common.LogFactory;
import org.jumpmind.symmetric.core.common.NotImplementedException;
import org.jumpmind.symmetric.core.common.StringUtils;
import org.jumpmind.symmetric.core.model.Column;
import org.jumpmind.symmetric.core.model.DataEventType;
import org.jumpmind.symmetric.core.model.Table;
import org.jumpmind.symmetric.core.model.Trigger;
import org.jumpmind.symmetric.core.model.TriggerHistory;
import org.jumpmind.symmetric.core.process.sql.TableToExtract;

abstract public class AbstractDataCaptureBuilder implements IDataCaptureBuilder {

    protected final Log log = LogFactory.getLog(getClass());

    private static final String ORIG_TABLE_ALIAS = "orig";

    protected IDbDialect dbDialect;

    public AbstractDataCaptureBuilder(IDbDialect dbDialect) {
        this.dbDialect = dbDialect;
    }

    // TODO make a single method to get a template for a specific column type.
    // abstract protected String getColumnTemplate(int type);

    abstract protected String getClobColumnTemplate();

    abstract protected String getNewTriggerValue();

    abstract protected String getOldTriggerValue();

    abstract protected String getBlobColumnTemplate();

    abstract protected String getWrappedBlobColumnTemplate();

    protected String getTableExtractCountSqlTemplate() {
        return "select count(*) from $(schemaName)$(tableName) t where $(whereClause)";
    }

    abstract protected Map<String, String> getFunctionTemplatesToInstall();

    abstract protected String getFunctionInstalledSqlTemplate();

    protected String getEmptyColumnTemplate() {return null;}

    abstract protected String getStringColumnTemplate();

    protected String getXmlColumnTemplate() {return null;}

    protected String getArrayColumnTemplate() {return null;}

    abstract protected String getNumberColumnTemplate();

    abstract protected String getDateTimeColumnTemplate();
    
    abstract protected String getDateTimeWithTimeZoneTemplate();

    abstract protected String getBooleanColumnTemplate();

    abstract protected String getTimeColumnTemplate();

    abstract protected String getDateColumnTemplate();

    abstract protected String getTriggerConcatCharacter();

    protected String getOldColumnPrefix()  {return null;}

    protected String getNewColumnPrefix() {return null;}

    abstract protected String getInsertTriggerTemplate();

    abstract protected String getUpdateTriggerTemplate();

    abstract protected String getDeleteTriggerTemplate();
    
    protected String getPostInsertTriggerTemplate() {return null;}

    protected String getPostUpdateTriggerTemplate() {return null;}

    protected String getPostDeleteTriggerTemplate() {return null;}

    abstract protected String getTransactionTriggerExpression();

    protected boolean isTransactionIdOverrideSupported() {
        return true;
    }

    protected String preProcessTriggerSqlClause(String sqlClause) {
        return sqlClause;
    }

    abstract protected String getDataHasChangedCondition();

    abstract protected String getSyncTriggersExpression();

    protected String getTableExtractSqlTableAlias() {
        return "t";
    }

    protected String massageForLobs(String sql, boolean supportsBigLobs) {
        return sql;
    }

    protected boolean isEscapeForDatabaseInserts() {
        return false;
    }

    protected boolean doesLobNeedSelectedFromTriggers() {
        return false;
    }

    protected String getTableExtractSqlTemplate() {
        return "select $(columns) from $(schemaName)$(tableName) t where $(whereClause)";
    }    

    private ColumnString buildColumnString(String origTableAlias, String tableAlias,
            String columnPrefix, Column[] columns, boolean isOld, boolean supportsBigLobs,
            boolean concat) {
        String columnsText = "";
        boolean isLob = false;

        final String triggerConcatCharacter = getTriggerConcatCharacter();

        String lastCommandToken = null;
        if (concat) {
            lastCommandToken = isEscapeForDatabaseInserts() ? (triggerConcatCharacter + "'',''" + triggerConcatCharacter)
                    : (triggerConcatCharacter + "','" + triggerConcatCharacter);
        } else {
            lastCommandToken = ",";
        }

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
                    templateToUse = getNumberColumnTemplate();
                    break;
                case Types.CHAR:
                case Types.VARCHAR:
                case Types.LONGVARCHAR:
                    templateToUse = getStringColumnTemplate();
                    break;
                case Types.SQLXML:
                    templateToUse = getXmlColumnTemplate();
                    break;
                case Types.ARRAY:
                    templateToUse = getArrayColumnTemplate();
                    break;
                case Types.CLOB:
                    if (isOld && doesLobNeedSelectedFromTriggers()) {
                        templateToUse = getEmptyColumnTemplate();
                    } else {
                        templateToUse = getClobColumnTemplate();
                    }
                    isLob = true;
                    break;
                case Types.BLOB:
                    templateToUse = getWrappedBlobColumnTemplate();
                    if (templateToUse != null) {
                        isLob = true;
                        break;
                    }
                case Types.BINARY:
                case Types.VARBINARY:
                case Types.LONGVARBINARY:
                    // SQL-Server ntext binary type
                case -10:
                    if (isOld && doesLobNeedSelectedFromTriggers()) {
                        templateToUse = getEmptyColumnTemplate();
                    } else {
                        templateToUse = getBlobColumnTemplate();
                    }
                    isLob = true;
                    break;
                case Types.DATE:
                    if (noDateColumnTemplate()) {
                        templateToUse = getDateTimeColumnTemplate();
                    } else {
                        templateToUse = getDateColumnTemplate();
                    }
                    break;
                case Types.TIME:
                    if (noTimeColumnTemplate()) {
                        templateToUse = getDateTimeColumnTemplate();
                        break;
                    }
                    templateToUse = getTimeColumnTemplate();
                    break;
                case Types.TIMESTAMP:
                    templateToUse = getDateTimeColumnTemplate();
                    break;
                case Types.BOOLEAN:
                case Types.BIT:
                    templateToUse = getBooleanColumnTemplate();
                    break;
                case -101:
                    if (StringUtils.isNotBlank(this.getDateTimeWithTimeZoneTemplate())) {
                        templateToUse = this.getDateTimeWithTimeZoneTemplate();
                        break;
                    }   
                case Types.NULL:
                case Types.OTHER:
                case Types.JAVA_OBJECT:
                case Types.DISTINCT:
                case Types.STRUCT:
                case Types.REF:
                case Types.DATALINK:
                default:
                    throw new NotImplementedException(column.getName() + " is of type "
                            + column.getType());
                }

                if (templateToUse != null) {
                    templateToUse = templateToUse.trim();
                } else {
                    throw new NotImplementedException(column.getName() + " is of type "
                            + column.getType());
                }

                String formattedColumnText = StringUtils.replaceTokens("columnName",
                        String.format("%s%s", columnPrefix, column.getName()), templateToUse);

                if (isLob) {
                    formattedColumnText = massageForLobs(formattedColumnText, supportsBigLobs);
                }

                String columnAlias = "";
                if (!concat) {
                    columnAlias = " AS " + column.getName();
                }
                columnsText = columnsText + "\n          " + formattedColumnText + columnAlias
                        + lastCommandToken;
            }

        }

        if (columnsText.endsWith(lastCommandToken)) {
            columnsText = columnsText
                    .substring(0, columnsText.length() - lastCommandToken.length());
        }

        columnsText = StringUtils.replaceTokens("origTableAlias", origTableAlias, columnsText);
        columnsText = StringUtils.replaceTokens("tableAlias", tableAlias, columnsText);
        return new ColumnString(columnsText, isLob);
    }

    // TODO: move to DerbySqlTemplate or change language for use in all DBMSes
    private String getPrimaryKeyWhereString(String alias, Column[] columns) {

        final String triggerConcatCharacter = getTriggerConcatCharacter();

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

    protected String replaceDefaultSchemaAndCatalog(Table table, String sql) {
        String defaultCatalog = dbDialect.getDefaultCatalog();
        String defaultSchema = dbDialect.getDefaultSchema();

        boolean resolveSchemaAndCatalogs = table.getCatalogName() != null
                || table.getSchemaName() != null;

        sql = StringUtils.replaceTokens("defaultSchema", resolveSchemaAndCatalogs
                && defaultSchema != null && defaultSchema.length() > 0 ? defaultSchema + "." : "",
                sql);

        return StringUtils.replaceTokens("defaultCatalog", resolveSchemaAndCatalogs
                && defaultCatalog != null && defaultCatalog.length() > 0 ? defaultCatalog + "."
                : "", sql);
    }

    public String createTableExtractSql(TableToExtract tableToExtract, boolean supportsBigLobs) {
        return createTableExtractSql(tableToExtract, null, supportsBigLobs);
    }

    public String createTableExtractSql(TableToExtract tableToExtract,
            Map<String, String> replacementTokens, boolean supportsBigLobs) {
        return replaceTemplateVariables(getTableExtractSqlTemplate(), tableToExtract,
                replacementTokens, supportsBigLobs, false);
    }

    public String createTableExtractCountSql(TableToExtract tableToExtract,
            Map<String, String> replacementTokens) {
        return replaceTemplateVariables(getTableExtractCountSqlTemplate(), tableToExtract,
                replacementTokens, false, false);
    }

    protected String replaceTemplateVariables(String sql, TableToExtract tableToExtract,
            Map<String, String> replacementTokens, boolean supportsBigLobs, boolean concatColumns) {
        Column[] columns = tableToExtract.getTable().getColumns();
        String columnsText = buildColumnString(getTableExtractSqlTableAlias(),
                getTableExtractSqlTableAlias(), "", columns, false, supportsBigLobs, concatColumns).columnString;
        sql = StringUtils.replaceTokens("columns", columnsText, sql);
        sql = StringUtils
                .replaceTokens(
                        "whereClause",
                        StringUtils.isBlank(tableToExtract.getCondition()) ? SqlConstants.ALWAYS_TRUE_CONDITION
                                : tableToExtract.getCondition(), sql);
        sql = StringUtils.replaceTokens("tableName", tableToExtract.getTable().getTableName(), sql);
        sql = StringUtils.replaceTokens("schemaName", tableToExtract.getTable()
                .getQualifiedTablePrefix(dbDialect.getDbDialectInfo().getIdentifierQuoteString()), sql);
        sql = StringUtils.replaceTokens(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(getTableExtractSqlTableAlias(), tableToExtract.getTable()
                        .getPrimaryKeyColumnsArray()), sql);

        // Replace these parameters to give the initiaLoadContition a chance to
        // reference the node that is being loaded
        sql = StringUtils.replaceTokens(sql, replacementTokens, true);
        sql = replaceDefaultSchemaAndCatalog(tableToExtract.getTable(), sql);

        return sql;
    }

    public String createCsvPrimaryKeySql(TableToExtract tableToExtract,
            Map<String, String> replacementTokens, boolean supportsBigLobs) {
        String sql = getTableExtractSqlTemplate();

        Column[] columns = tableToExtract.getTable().getPrimaryKeyColumnsArray();
        String columnsText = buildColumnString(getTableExtractSqlTableAlias(),
                getTableExtractSqlTableAlias(), "", columns, false, supportsBigLobs, true)
                .toString();
        sql = StringUtils.replaceTokens("columns", columnsText, sql);

        sql = StringUtils.replaceTokens("tableName", tableToExtract.getTable().getTableName(), sql);
        sql = StringUtils.replaceTokens("schemaName",
                tableToExtract.getTable().getSchemaName() != null ? tableToExtract.getTable()
                        .getSchemaName() + "." : "", sql);
        sql = StringUtils.replaceTokens("whereClause", tableToExtract.getCondition(), sql);
        sql = StringUtils.replaceTokens("primaryKeyWhereString",
                getPrimaryKeyWhereString(getTableExtractSqlTableAlias(), columns), sql);

        return sql;
    }

    public String[] getFunctionsToInstall() {
        Map<String, String> functionTemplatesToInstall = getFunctionTemplatesToInstall();
        if (functionTemplatesToInstall != null) {
            return functionTemplatesToInstall.keySet().toArray(
                    new String[functionTemplatesToInstall.size()]);
        } else {
            return new String[0];
        }
    }

    protected String replaceTemplateVariables(DataEventType dml, TriggerHistory history,
            String tablePrefix, Table metaData, String ddl, boolean supportsBigLobs,
            boolean concatColumns) {

        Trigger trigger = history.getTrigger();
        boolean resolveSchemaAndCatalogs = history.getSourceCatalogName() != null
                || history.getSourceSchemaName() != null;

        ddl = StringUtils.replaceTokens("targetTableName", metaData.getTableName(), ddl);

        ddl = StringUtils.replaceTokens("triggerName", history.getTriggerNameForDmlType(dml), ddl);
        ddl = StringUtils.replaceTokens("prefixName", tablePrefix, ddl);
        ddl = StringUtils.replaceTokens("channelName", history.getTrigger().getChannelId(), ddl);
        ddl = StringUtils.replaceTokens("triggerHistoryId",
                Integer.toString(history == null ? -1 : history.getTriggerHistoryId()), ddl);
        String triggerExpression = getTransactionTriggerExpression();
        if (isTransactionIdOverrideSupported() && !StringUtils.isBlank(trigger.getTxIdExpression())) {
            triggerExpression = trigger.getTxIdExpression();
        }
        ddl = StringUtils.replaceTokens("txIdExpression",
                preProcessTriggerSqlClause(triggerExpression), ddl);

        ddl = StringUtils.replaceTokens("externalSelect",
                (trigger.getExternalSelect() == null ? "null" : "("
                        + preProcessTriggerSqlClause(trigger.getExternalSelect()) + ")"), ddl);

        ddl = StringUtils.replaceTokens("syncOnInsertCondition",
                preProcessTriggerSqlClause(trigger.getSyncOnInsertCondition()), ddl);
        ddl = StringUtils.replaceTokens("syncOnUpdateCondition",
                preProcessTriggerSqlClause(trigger.getSyncOnUpdateCondition()), ddl);
        ddl = StringUtils.replaceTokens("syncOnDeleteCondition",
                preProcessTriggerSqlClause(trigger.getSyncOnDeleteCondition()), ddl);
        ddl = StringUtils.replaceTokens("dataHasChangedCondition",
                preProcessTriggerSqlClause(getDataHasChangedCondition()), ddl);

        String defaultCatalog = dbDialect.getDefaultCatalog();
        String defaultSchema = dbDialect.getDefaultSchema();

        String syncTriggersExpression = getSyncTriggersExpression();
        syncTriggersExpression = StringUtils
                .replaceTokens("defaultCatalog", resolveSchemaAndCatalogs && defaultCatalog != null
                        && defaultCatalog.length() > 0 ? defaultCatalog + "." : "",
                        syncTriggersExpression);
        syncTriggersExpression = StringUtils
                .replaceTokens("defaultSchema", resolveSchemaAndCatalogs && defaultSchema != null
                        && defaultSchema.length() > 0 ? defaultSchema + "." : "",
                        syncTriggersExpression);
        ddl = StringUtils.replaceTokens("syncOnIncomingBatchCondition", trigger
                .isSyncOnIncomingBatch() ? SqlConstants.ALWAYS_TRUE_CONDITION
                : syncTriggersExpression, ddl);
        ddl = StringUtils.replaceTokens("origTableAlias", ORIG_TABLE_ALIAS, ddl);

        Column[] columns = trigger.orderColumnsForTable(metaData);
        ColumnString columnString = buildColumnString(ORIG_TABLE_ALIAS, getNewTriggerValue(),
                getNewColumnPrefix(), columns, false, supportsBigLobs, concatColumns);
        ddl = StringUtils.replaceTokens("columns", columnString.toString(), ddl);

        ddl = replaceDefaultSchemaAndCatalog(metaData, ddl);

        ddl = StringUtils.replaceTokens(
                "virtualOldNewTable",
                buildVirtualTableSql(getOldColumnPrefix(), getNewColumnPrefix(),
                        metaData.getColumns()), ddl);
        ddl = StringUtils.replaceTokens(
                "oldColumns",
                buildColumnString(ORIG_TABLE_ALIAS, getOldTriggerValue(), getOldColumnPrefix(),
                        columns, true, supportsBigLobs, concatColumns).toString(), ddl);
        ddl = eval(columnString.isBlobClob, "containsBlobClobColumns", ddl);

        // some column templates need tableName and schemaName
        ddl = StringUtils.replaceTokens("tableName", history == null ? trigger.getSourceTableName()
                : history.getSourceTableName(), ddl);
        ddl = StringUtils.replaceTokens("schemaName",
                (history == null ? (resolveSchemaAndCatalogs
                        && trigger.getSourceSchemaName() != null ? trigger.getSourceSchemaName()
                        + "." : "") : (resolveSchemaAndCatalogs
                        && history.getSourceSchemaName() != null ? history.getSourceSchemaName()
                        + "." : "")), ddl);

        columns = metaData.getPrimaryKeyColumnsArray();
        ddl = StringUtils.replaceTokens(
                "oldKeys",
                buildColumnString(ORIG_TABLE_ALIAS, getOldTriggerValue(), getOldColumnPrefix(),
                        columns, true, supportsBigLobs, concatColumns).toString(), ddl);
        ddl = StringUtils.replaceTokens("oldNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoin(getOldTriggerValue(), getNewTriggerValue(), columns), ddl);
        ddl = StringUtils.replaceTokens("tableNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoin(ORIG_TABLE_ALIAS, getNewTriggerValue(), columns), ddl);
        ddl = StringUtils.replaceTokens(
                "primaryKeyWhereString",
                getPrimaryKeyWhereString(dml == DataEventType.DELETE ? getOldTriggerValue()
                        : getNewTriggerValue(), columns), ddl);

        ddl = StringUtils.replaceTokens("declareOldKeyVariables",
                buildKeyVariablesDeclare(columns, "old"), ddl);
        ddl = StringUtils.replaceTokens("declareNewKeyVariables",
                buildKeyVariablesDeclare(columns, "new"), ddl);
        ddl = StringUtils.replaceTokens("oldKeyNames",
                buildColumnNameString(getOldTriggerValue(), columns), ddl);
        ddl = StringUtils.replaceTokens("newKeyNames",
                buildColumnNameString(getNewTriggerValue(), columns), ddl);
        ddl = StringUtils.replaceTokens("oldKeyVariables", buildKeyVariablesString(columns, "old"),
                ddl);
        ddl = StringUtils.replaceTokens("newKeyVariables", buildKeyVariablesString(columns, "new"),
                ddl);
        ddl = StringUtils.replaceTokens("varNewPrimaryKeyJoin",
                aliasedPrimaryKeyJoinVar(getNewTriggerValue(), "new", columns), ddl);
        ddl = StringUtils.replaceTokens("varOldPrimaryKeyJoin",
                aliasedPrimaryKeyJoinVar(getOldTriggerValue(), "old", columns), ddl);

        // replace $(newTriggerValue) and $(oldTriggerValue)
        ddl = StringUtils.replaceTokens("newTriggerValue", getNewTriggerValue(), ddl);
        ddl = StringUtils.replaceTokens("oldTriggerValue", getOldTriggerValue(), ddl);
        ddl = StringUtils.replaceTokens("newColumnPrefix", getNewColumnPrefix(), ddl);
        ddl = StringUtils.replaceTokens("oldColumnPrefix", getOldColumnPrefix(), ddl);
        switch (dml) {
        case DELETE:
            ddl = StringUtils.replaceTokens("curTriggerValue", getOldTriggerValue(), ddl);
            ddl = StringUtils.replaceTokens("curColumnPrefix", getOldColumnPrefix(), ddl);
            break;
        case INSERT:
        case UPDATE:
        default:
            ddl = StringUtils.replaceTokens("curTriggerValue", getNewTriggerValue(), ddl);
            ddl = StringUtils.replaceTokens("curColumnPrefix", getNewColumnPrefix(), ddl);
            break;
        }
        return ddl;
    }

    public String createTriggerDDL(DataEventType dml, Trigger trigger, TriggerHistory history,
            String tablePrefix, Table metaData, boolean supportsBigLobs) {
        String ddl = null;
        switch (dml) {
        case INSERT:
            ddl = getInsertTriggerTemplate();
            break;
        case UPDATE:
            ddl = getUpdateTriggerTemplate();
            break;
        case DELETE:
            ddl = getDeleteTriggerTemplate();
            break;
        }
        if (ddl == null) {
            throw new NotImplementedException(dml.name() + " trigger is not implemented by "
                    + dbDialect.getClass().getName());
        }
        return replaceTemplateVariables(dml, history, tablePrefix, metaData, ddl, supportsBigLobs,
                true);
    }

    public String createPostTriggerDDL(DataEventType dml, TriggerHistory history,
            String tablePrefix, Table metaData, boolean supportsBigLobs) {
        String ddl = null;
        switch (dml) {
        case INSERT:
            ddl = getPostInsertTriggerTemplate();
            break;
        case UPDATE:
            ddl = getPostUpdateTriggerTemplate();
            break;
        case DELETE:
            ddl = getPostDeleteTriggerTemplate();
            break;
        }
        return replaceTemplateVariables(dml, history, tablePrefix, metaData, ddl, supportsBigLobs,
                true);
    }

    private String buildVirtualTableSql(String oldTriggerValue, String newTriggerValue,
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

    private boolean noTimeColumnTemplate() {
        String timeColumnTemplate = getTimeColumnTemplate();
        return timeColumnTemplate == null || timeColumnTemplate.equals("null")
                || timeColumnTemplate.trim().equals("");
    }

    private boolean noDateColumnTemplate() {
        return StringUtils.isBlank(getDateColumnTemplate());
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
            default:
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

    public String getFunctionSql(String functionKey, String functionName, String defaultSchema) {
        Map<String, String> functionTemplatesToInstall = getFunctionTemplatesToInstall();
        if (functionTemplatesToInstall != null) {
            String ddl = StringUtils.replaceTokens("functionName", functionName,
                    functionTemplatesToInstall.get(functionKey));
            ddl = StringUtils.replaceTokens("version", Version.versionWithUnderscores(), ddl);
            ddl = StringUtils.replaceTokens("defaultSchema",
                    defaultSchema != null && defaultSchema.length() > 0 ? defaultSchema + "." : "",
                    ddl);
            return ddl;
        } else {
            return null;
        }
    }

    public String getFunctionInstalledSql(String functionName, String defaultSchema) {
        String functionInstalledSql = getFunctionInstalledSqlTemplate();
        if (functionInstalledSql != null) {
            String ddl = StringUtils.replaceTokens("functionName", functionName,
                    functionInstalledSql);
            ddl = StringUtils.replaceTokens("version", Version.versionWithUnderscores(), ddl);
            ddl = StringUtils.replaceTokens("defaultSchema",
                    defaultSchema != null && defaultSchema.length() > 0 ? defaultSchema + "." : "",
                    ddl);
            return ddl;
        } else {
            return null;
        }
    }

    public void configureRequiredFunctions() {
        ISqlTemplate sqlTemplate = dbDialect.getSqlTemplate();
        String[] functions = getFunctionsToInstall();
        for (int i = 0; i < functions.length; i++) {
            String funcName = String.format("%s_%s", dbDialect.getParameters().getTablePrefix(),
                    functions[i]);
            if (sqlTemplate.queryForInt(getFunctionInstalledSql(funcName,
                    dbDialect.getDefaultSchema())) == 0) {
                sqlTemplate.update(getFunctionSql(functions[i], funcName,
                        dbDialect.getDefaultSchema()));
                log.info("Installed function %s", funcName);
            }
        }
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
