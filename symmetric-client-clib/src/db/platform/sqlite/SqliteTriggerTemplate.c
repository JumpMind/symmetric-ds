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
#include "db/sqlite/SqliteTriggerTemplate.h"

char * SymSqliteTriggerTemplate_fillOutColumnTemplate(SymSqliteTriggerTemplate *this,
        char *origTableAlias, char *tableAlias,
        char *columnPrefix, SymColumn *column, SymDataEventType dml, unsigned short isOld,
        SymChannel *channel, SymTrigger *trigger) {

    char *templateToUse;

    switch (column->sqlType) {
    case SYM_SQL_TYPE_BIT:
    case SYM_SQL_TYPE_BOOLEAN:
        templateToUse = "case when %s.%s is null then '' when %s.%s = 1 then '\"1\"' else '\"0\"' end";
        break;
    case SYM_SQL_TYPE_TINYINT:
    case SYM_SQL_TYPE_SMALLINT:
    case SYM_SQL_TYPE_INTEGER:
    case SYM_SQL_TYPE_BIGINT:
    case SYM_SQL_TYPE_FLOAT:
    case SYM_SQL_TYPE_DOUBLE:
    case SYM_SQL_TYPE_NUMERIC:
    case SYM_SQL_TYPE_DECIMAL:
    case SYM_SQL_TYPE_REAL:
        templateToUse = "case when %s.%s is null then '' else ('\"' || cast(%s.%s as varchar) || '\"') end";
        break;
    case SYM_SQL_TYPE_CHAR:
    case SYM_SQL_TYPE_NCHAR:
    case SYM_SQL_TYPE_VARCHAR:
    case SYM_SQL_TYPE_NVARCHAR:
    case SYM_SQL_TYPE_CLOB:
    case SYM_SQL_TYPE_LONGVARCHAR:
    case SYM_SQL_TYPE_LONGNVARCHAR:
        templateToUse = "case when %s.%s is null then '' else '\"' || replace(replace(%s.%s,'\\','\\\\'),'\"','\\\"') || '\"' end";
        break;
    case SYM_SQL_TYPE_BLOB:
    case SYM_SQL_TYPE_LONGVARBINARY:
        templateToUse = "case when %s.%s is null then '' else '\"' || replace(replace(hex(%s.%s),'\\','\\\\'),'\"','\\\"') || '\"' end ";
        break;
    case SYM_SQL_TYPE_DATE:
    case SYM_SQL_TYPE_TIMESTAMP:
    case SYM_SQL_TYPE_TIME:
        templateToUse = "case when strftime('%%Y-%%m-%%d %%H:%%M:%%f',%s.%s) is null then '' else ('\"' || strftime('%%Y-%%m-%%d %%H:%%M:%%f', %s.%s) || '\"') end";
        break;
    default:
        templateToUse = NULL;
        SymLog_error("Unknown sqlType %d", column->sqlType);
        break;
    }

    char* columnName = column->name;
    char* formattedColumnText =
            SymStringUtils_format(templateToUse, tableAlias, columnName, tableAlias, columnName);

    return formattedColumnText;
}

char * SymSqliteTriggerTemplate_buildColumnsString(SymSqliteTriggerTemplate *this, char *origTableAlias, char *tableAlias,
        char *columnPrefix, SymList *columns, SymDataEventType dml, unsigned int isOld,
        SymChannel *channel, SymTrigger *trigger) {

    char *lastCommandToken = "||','||";

    SymStringBuilder *buff = SymStringBuilder_new(NULL);

    int i;
    for (i = 0; i < columns->size; i++) {
        SymColumn *column = columns->get(columns, i);
        char *columnString = SymSqliteTriggerTemplate_fillOutColumnTemplate(this, origTableAlias,
                tableAlias, columnPrefix, column, dml, isOld, channel, trigger);
        buff->append(buff, columnString);
        buff->append(buff, "\n");
        if (i < (columns->size-1)) {
            buff->append(buff, lastCommandToken);
        }
       free(columnString);
    }

    return buff->destroyAndReturn(buff);
}


char * SymSqliteTriggerTemplate_replaceTemplateVariables(SymSqliteTriggerTemplate  *this, SymDataEventType dml,
        SymTrigger *trigger, SymTriggerHistory *history, SymChannel *channel, char *tablePrefix,
        SymTable *table, char *defaultCatalog, char *defaultSchema, char *ddl) {
    SymList *primaryKeyColumns = table->getPrimaryKeyColumns(table);

    char *triggerName = history->getTriggerNameForDmlType(history, dml);
    char *schemaName = "";
    char *tableName = table->name;
    char *syncOnInsertCondition = "1"; // TODO
    char *syncOnUpdateCondition = "1"; // TODO
    char *syncOnDeleteCondition = "1"; // TODO
    char *syncOnIncomingBatchCondition = "1"; // TODO
    char *targetTableName = trigger->sourceTableName; // TODO
    char *triggerHistoryId = SymStringUtils_format("%d", history == NULL ? -1 : history->triggerHistoryId);
    char *oldKeys = SymSqliteTriggerTemplate_buildColumnsString(this, SYM_ORIG_TABLE_ALIAS,
            "old", "", primaryKeyColumns, dml, 1, channel, trigger);
    char *oldColumns = trigger->useCaptureOldData ? SymSqliteTriggerTemplate_buildColumnsString(this, SYM_ORIG_TABLE_ALIAS,
            "old", "old", table->columns, dml, 1, channel, trigger) : SymStringUtils_format("%s", "null");
    char *columns = SymSqliteTriggerTemplate_buildColumnsString(this, SYM_ORIG_TABLE_ALIAS,
            "new", "", table->columns, dml, 0, channel, trigger);
    char *channelExpression = SymStringUtils_format("'%s'", trigger->channelId); // TODO
    char *sourceNodeExpression = "null"; // TODO
    char *externalSelect = "null"; // TODO
    char *custom_on_insert_text = ""; // TODO
    char *custom_on_update_text = ""; // TODO
    char *custom_on_delete_text = ""; // TODO

    char *formattedDdl = NULL;

    if (dml == SYM_DATA_EVENT_INSERT) {
        formattedDdl = SymStringUtils_format(ddl, triggerName, schemaName, tableName, syncOnInsertCondition,
                syncOnIncomingBatchCondition, targetTableName, triggerHistoryId, columns, channelExpression, sourceNodeExpression,
                externalSelect, custom_on_insert_text);

    } else if (dml == SYM_DATA_EVENT_UPDATE) {
        formattedDdl = SymStringUtils_format(ddl, triggerName, schemaName, tableName, syncOnUpdateCondition,
                syncOnIncomingBatchCondition, targetTableName, triggerHistoryId, oldKeys, columns, oldColumns, channelExpression, sourceNodeExpression,
                externalSelect, custom_on_update_text);
    }
    else if (dml == SYM_DATA_EVENT_DELETE) {
        formattedDdl = SymStringUtils_format(ddl, triggerName, schemaName, tableName, syncOnDeleteCondition,
                syncOnIncomingBatchCondition, targetTableName, triggerHistoryId, oldKeys, oldColumns, channelExpression, sourceNodeExpression,
                externalSelect, custom_on_delete_text);
    }

    free(triggerHistoryId);
    free(oldKeys);
    free(oldColumns);
    free(columns);
    free(channelExpression);
    primaryKeyColumns->destroy(primaryKeyColumns);

    return formattedDdl;
}

char * SymSqliteTriggerTemplate_createTriggerDDL(SymSqliteTriggerTemplate *this, SymDataEventType dml,
        SymTrigger *trigger, SymTriggerHistory *history, SymChannel *channel, char *tablePrefix,
        SymTable *originalTable, char *defaultCatalog, char *defaultSchema) {

    char *ddl = NULL;
    if (dml == SYM_DATA_EVENT_INSERT) {
        ddl = SYM_SQL_SQLITE_INSERT_TRIGGER_TEMPLATE;
    } else if (dml == SYM_DATA_EVENT_UPDATE) {
        ddl = SYM_SQL_SQLITE_UPDATE_TRIGGER_TEMPLATE;
    } else if (dml == SYM_DATA_EVENT_DELETE) {
        ddl = SYM_SQL_SQLITE_DELETE_TRIGGER_TEMPLATE;
    } else {
        SymLog_error("Unknown dml %d", dml);
    }

    char *formattedDdl = SymSqliteTriggerTemplate_replaceTemplateVariables(this, dml,
            trigger, history, channel, tablePrefix, originalTable, defaultCatalog,
            defaultSchema, ddl);

    return formattedDdl;
}

long SymSqliteTriggerTemplate_toHashedValue(SymSqliteTriggerTemplate *this) {
    long hashCode = SymStringBuilder_hashCode(SYM_SQL_SQLITE_INSERT_TRIGGER_TEMPLATE);
    hashCode += SymStringBuilder_hashCode(SYM_SQL_SQLITE_UPDATE_TRIGGER_TEMPLATE);
    hashCode += SymStringBuilder_hashCode(SYM_SQL_SQLITE_DELETE_TRIGGER_TEMPLATE);
    return hashCode;
}

void SymSqliteTriggerTemplate_destroy(SymSqliteTriggerTemplate *this) {

    free(this);
}

SymSqliteTriggerTemplate * SymSqliteTriggerTemplate_new(SymSqliteTriggerTemplate *this) {
    if (this == NULL) {
        this = (SymSqliteTriggerTemplate *) calloc(1, sizeof(SymSqliteTriggerTemplate));
    }
    SymTriggerTemplate_new(&this->super);
    SymTriggerTemplate *super = &this->super;
    super->createTriggerDDL = (void *) &SymSqliteTriggerTemplate_createTriggerDDL;
    super->toHashedValue = (void *) &SymSqliteTriggerTemplate_toHashedValue;
    super->destroy = (void *) &SymSqliteTriggerTemplate_destroy;
    return this;
}
