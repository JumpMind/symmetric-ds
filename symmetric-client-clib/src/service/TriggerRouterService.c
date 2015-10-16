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
#include "service/TriggerRouterService.h"

static SymTriggerRouter * SymTriggerRouterService_triggerRouterMapper(SymRow *row) {
    SymTriggerRouter *triggerRouter = SymTriggerRouter_new(NULL);

    triggerRouter->trigger = SymTrigger_new(NULL);
    triggerRouter->trigger->triggerId = row->getStringNew(row, "trigger_id");

    triggerRouter->router = SymRouter_new(NULL);
    triggerRouter->router->routerId = row->getStringNew(row, "router_id");

    triggerRouter->enabled = row->getBoolean(row, "enabled");
    triggerRouter->initialLoadOrder = row->getInt(row, "initial_load_order");
    triggerRouter->initialLoadSelect = row->getStringNew(row, "initial_load_select");
    triggerRouter->initialLoadDeleteStmt = row->getStringNew(row, "initial_load_delete_stmt");
    triggerRouter->initialLoadBatchCount = row->getInt(row, "initial_load_batch_count");
    triggerRouter->createTime = row->getDate(row, "create_time");
    triggerRouter->lastUpdateTime = row->getDate(row, "last_update_time");
    triggerRouter->lastUpdateBy = row->getStringNew(row, "last_update_by");
    triggerRouter->pingBackEnabled = row->getBoolean(row, "pingBackEnabled");

    return triggerRouter;
}

static SymTrigger * SymTriggerRouterService_triggerMapper(SymRow *row) {
    SymTrigger *trigger = SymTrigger_new(NULL);

    trigger->triggerId = row->getStringNew(row, "trigger_id");
    trigger->channelId = row->getStringNew(row, "channel_id");
    trigger->reloadChannelId = row->getStringNew(row, "reload_channel_id");
    trigger->sourceTableName = row->getStringNew(row, "source_table_name");
    trigger->syncOnInsert = row->getBoolean(row, "sync_on_insert");
    trigger->syncOnUpdate = row->getBoolean(row, "sync_on_update");
    trigger->syncOnDelete = row->getBoolean(row, "sync_on_delete");
    trigger->syncOnIncomingBatch = row->getBoolean(row, "sync_on_incoming_batch");
    trigger->useStreamLobs = row->getBoolean(row, "use_stream_lobs");
    trigger->useCaptureLobs = row->getBoolean(row, "use_capture_lobs");
    trigger->useCaptureOldData = row->getBoolean(row, "use_capture_old_data");
    trigger->useHandleKeyUpdates = row->getBoolean(row, "use_handle_key_updates");
    trigger->nameForDeleteTrigger = row->getStringNew(row, "name_for_delete_trigger");
    trigger->nameForInsertTrigger = row->getStringNew(row, "name_for_insert_trigger");
    trigger->nameForUpdateTrigger = row->getStringNew(row, "name_for_update_trigger");
    char *schema = row->getStringNew(row, "source_schema_name");
    trigger->sourceSchemaName = schema;
    char *catalog = row->getStringNew(row, "source_catalog_name");
    trigger->sourceCatalogName = catalog;

    char *condition = row->getStringNew(row, "sync_on_insert_condition");
    if (!SymStringUtils_isBlank(condition)) {
        trigger->syncOnInsertCondition = condition;
    }
    condition = row->getStringNew(row, "sync_on_update_condition");
    if (!SymStringUtils_isBlank(condition)) {
        trigger->syncOnUpdateCondition = condition;
    }
    condition = row->getStringNew(row, "sync_on_delete_condition");
    if (!SymStringUtils_isBlank(condition)) {
        trigger->syncOnDeleteCondition = condition;
    }

    char *text = row->getStringNew(row, "custom_on_insert_text");
    if (!SymStringUtils_isBlank(text)) {
        trigger->customOnInsertText = text;
    }
    text = row->getStringNew(row, "custom_on_update_text");
    if (!SymStringUtils_isBlank(text)) {
        trigger->customOnUpdateText = text;
    }
    text = row->getStringNew(row, "custom_on_delete_text");
    if (!SymStringUtils_isBlank(text)) {
        trigger->customOnDeleteText = text;
    }

    condition = row->getStringNew(row, "external_select");
    if (!SymStringUtils_isBlank(condition)) {
        trigger->externalSelect = condition;
    }

    trigger->channelExpression = row->getStringNew(row, "channel_expression");
    trigger->txIdExpression = row->getStringNew(row, "tx_id_expression");

    trigger->createTime = row->getDate(row, "t_create_time");
    trigger->lastUpdateTime = row->getDate(row, "t_last_update_time");
    trigger->lastUpdateBy = row->getStringNew(row, "t_last_update_by");
    trigger->excludedColumnNames = row->getStringNew(row, "excluded_column_names");
    trigger->syncKeyNames = row->getStringNew(row, "sync_key_names");

    return trigger;
}

static SymRouter * SymTriggerRouterService_routerMapper(SymRow *row) {

    SymRouter *router = SymRouter_new(NULL);

    SymNodeGroupLink_new(NULL);

    router->syncOnInsert = row->getBoolean(row, "r_sync_on_insert");
    router->syncOnUpdate = row->getBoolean(row, "r_sync_on_update");
    router->syncOnDelete = row->getBoolean(row, "r_sync_on_delete");
    router->targetCatalogName = row->getStringNew(row, "target_catalog_name");

    SymNodeGroupLink *nodeGroupLink = SymNodeGroupLink_new(NULL);
    nodeGroupLink->sourceNodeGroupId = row->getStringNew(row, "source_node_group_id");
    nodeGroupLink->targetNodeGroupId = row->getStringNew(row, "target_node_group_id");
    router->nodeGroupLink = nodeGroupLink;

    router->targetSchemaName = row->getStringNew(row, "target_schema_name");
    router->targetTableName = row->getStringNew(row, "target_table_name");

    char *condition = row->getStringNew(row, "router_expression");
    if (!SymStringUtils_isBlank(condition)) {
        router->routerExpression = condition;
    }
    router->routerType = row->getStringNew(row, "router_type");
    router->routerId = row->getStringNew(row, "router_id");
    router->useSourceCatalogSchema = row->getBoolean(row, "use_source_catalog_schema");
    router->createTime = row->getDate(row, "r_create_time");
    router->lastUpdateTime = row->getDate(row, "r_last_update_time");
    router->lastUpdateBy = row->getStringNew(row, "r_last_update_by");
    return router;
}

static SymTriggerHistory * SymTriggerRouterService_triggerHistoryMapper(SymRow *row) {

    SymTriggerHistory *hist = SymTriggerHistory_new(NULL);
    hist->triggerHistoryId = row->getInt(row, "trigger_hist_id");
    hist->triggerId = row->getStringNew(row, "trigger_id");
    hist->sourceTableName = row->getStringNew(row, "source_table_name");
    hist->tableHash = row->getInt(row, "table_hash");
    hist->createTime = row->getDate(row, "create_time");
    hist->pkColumnNames = row->getStringNew(row, "pk_column_names");
    hist->columnNames = row->getStringNew(row, "column_names");
    hist->lastTriggerBuildReason = row->getStringNew(row, "last_trigger_build_reason");
    hist->nameForDeleteTrigger = row->getStringNew(row, "name_for_delete_trigger");
    hist->nameForInsertTrigger = row->getStringNew(row, "name_for_insert_trigger");
    hist->nameForUpdateTrigger = row->getStringNew(row, "name_for_update_trigger");
    hist->sourceSchemaName = row->getStringNew(row, "source_schema_name");
    hist->sourceCatalogName = row->getStringNew(row, "source_catalog_name");
    hist->triggerRowHash = row->getLong(row, "trigger_row_hash");
    hist->triggerTemplateHash = row->getLong(row, "trigger_template_hash");
    hist->errorMessage = row->getStringNew(row, "error_message");
    return hist;
}

SymRouter * SymTriggerRouterService_getRouterById(SymTriggerRouterService *this, char *routerId, unsigned short refreshCache) {
    long routerCacheTimeoutInMs = this->parameterService->getLong(this->parameterService, CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS, 600000);
    if (this->routersCache == NULL || refreshCache
            || time(NULL) * 1000 - this->routersCacheTime * 1000 > routerCacheTimeoutInMs) {
        this->routersCacheTime = time(NULL);
        SymList *routers = this->getRouters(this, 1);
        SymIterator *iter = routers->iterator(routers);
        while (iter->hasNext(iter)) {
            SymRouter *router = iter->next(iter);
            this->routersCache->put(this->routersCache, router->routerId, router, sizeof(SymRouter));
        }
    }
    return (SymRouter *) this->routersCache->get(this->routersCache, routerId);
}

SymList * SymTriggerRouterService_getRouters(SymTriggerRouterService *this, unsigned short replaceVariables) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sb = SymStringBuilder_newWithString("select ");
    sb->append(sb, SYM_SQL_SELECT_ROUTERS_COLUMN_LIST)->append(sb, SYM_SQL_SELECT_ROUTERS);
    int error;
    SymList *routers = sqlTemplate->query(sqlTemplate, sb->str, NULL, NULL, &error, (void *) SymTriggerRouterService_routerMapper);
    // TODO: handle replacement variables
    return routers;
}

SymTriggerHistory * SymTriggerRouterService_getTriggerHistory(SymTriggerRouterService *this, int histId) {
    SymTriggerHistory *history = this->historyMap->getByInt(this->historyMap, histId);
    if (history == NULL && histId >= 0) {
        SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
        SymStringArray *args = SymStringArray_new(NULL);
        args->addInt(args, histId);
        int error;
        history = sqlTemplate->queryForObject(sqlTemplate, SYM_SQL_TRIGGER_HIST, args, NULL, &error, (void *) SymTriggerRouterService_triggerHistoryMapper);
        this->historyMap->putByInt(this->historyMap, histId, history, sizeof(SymTriggerHistory));
        args->destroy(args);
    }
    return history;
}

SymList * SymTriggerRouterService_getActiveTriggerHistories(SymTriggerRouterService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_ALL_TRIGGER_HIST);
    sb->append(sb, SYM_SQL_ACTIVE_TRIGGER_HIST);
    int error;
    SymList *histories = sqlTemplate->query(sqlTemplate, sb->str, NULL, NULL, &error, (void *) SymTriggerRouterService_triggerHistoryMapper);
    sb->destroy(sb);

    SymIterator *iter = histories->iterator(histories);
    while (iter->hasNext(iter)) {
        SymTriggerHistory *triggerHistory = (SymTriggerHistory *) iter->next(iter);
        this->historyMap->putByInt(this->historyMap, triggerHistory->triggerHistoryId, triggerHistory, sizeof(SymTriggerHistory));
    }
    iter->destroy(iter);
    return histories;
}

SymList * SymTriggerRouterService_getActiveTriggerHistoriesByTrigger(SymTriggerRouterService *this, SymTrigger *trigger) {
    SymList *active = SymTriggerRouterService_getActiveTriggerHistories(this);
    SymList *list = SymList_new(NULL);
    SymIterator *iter = active->iterator(active);
    while (iter->hasNext(iter)) {
        SymTriggerHistory *triggerHistory = (SymTriggerHistory *) iter->next(iter);
        if (strcmp(triggerHistory->triggerId, trigger->triggerId) == 0) {
            list->add(list, triggerHistory);
        }
    }
    iter->destroy(iter);
    active->destroy(active);
    return list;
}

SymList * SymTriggerRouterService_getActiveTriggerHistoriesByTableName(SymTriggerRouterService *this, char *tableName) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, tableName);
    SymStringBuilder *sb = SymStringBuilder_newWithString(SYM_SQL_ALL_TRIGGER_HIST);
    sb->append(sb, SYM_SQL_TRIGGER_HIST_BY_SOURCE_TABLE_WHERE);
    int error;
    SymList *histories = sqlTemplate->query(sqlTemplate, sb->str, args, NULL, &error, (void *) SymTriggerRouterService_triggerHistoryMapper);
    args->destroy(args);
    sb->destroy(sb);
    return histories;
}

SymList * SymTriggerRouterService_getTriggers(SymTriggerRouterService *this, unsigned short replaceTokens) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    int error;
    SymList* triggers = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_TRIGGERS, NULL, NULL, &error, (void *) SymTriggerRouterService_triggerMapper);
    return triggers;
}

void SymTriggerRouterService_inactivateTriggerHistory(SymTriggerRouterService *this, SymTriggerHistory *history) {
    // TODO
}

unsigned short SymTriggerRouterService_isTriggerNameInUse(SymTriggerRouterService *this, SymList *activeTriggerHistories, char *triggerId, char *triggerName) {
    // TODO
    return 0;
}

SymTriggerHistory * SymTriggerRouterService_getNewestTriggerHistoryForTrigger(SymTriggerRouterService *this, char *triggerId, char *catalogName, char *schemaName, char *tableName) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_enhanceTriggerRouters(SymTriggerRouterService *this, SymList *triggerRouters) {
    SymMap *routersById = SymMap_new(NULL, 8);
    SymList *routers = SymTriggerRouterService_getRouters(this, 1);
    int i;
    for (i = 0; i < routers->size; i++) {
        SymRouter *router = routers->get(routers, i);
        char *routerId = SymStringUtils_toUpperCase(SymStringUtils_trim(router->routerId)); // TODO memory leak of strings.
        routersById->put(routersById, routerId, router, sizeof(SymRouter));
    }

    SymMap *triggersById = SymMap_new(NULL, 8);
    SymList *triggers = SymTriggerRouterService_getTriggers(this, 1);
    for (i = 0; i < triggers->size; i++) {
        SymTrigger *trigger = triggers->get(triggers, i);
        char *triggerId = SymStringUtils_toUpperCase(SymStringUtils_trim(trigger->triggerId));  // TODO memory leak of strings.
        triggersById->put(triggersById, triggerId, trigger, sizeof(SymTrigger));
    }

    for (i = 0; i < triggerRouters->size; i++) {
        SymTriggerRouter *triggerRouter = triggerRouters->get(triggerRouters, i);
        char* triggerid = SymStringUtils_toUpperCase(SymStringUtils_trim(triggerRouter->trigger->triggerId));  // TODO memory leak of strings.
        char* routerId = SymStringUtils_toUpperCase(SymStringUtils_trim(triggerRouter->router->routerId));  // TODO memory leak of strings.
        triggerRouter->trigger = triggersById->get(triggersById, triggerid );
        triggerRouter->router = routersById->get(routersById, routerId );
    }

    return triggerRouters;
}

SymList * SymTriggerRouterService_getTriggerRouters(SymTriggerRouterService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    int error;
    SymList* triggers = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_TRIGGER_ROUTERS, NULL, NULL, &error, (void *) SymTriggerRouterService_triggerRouterMapper);

    SymList *triggerRouters =
            SymTriggerRouterService_enhanceTriggerRouters(this, triggers);

    return triggerRouters;
}


void SymTriggerRouterService_insert(SymTriggerRouterService *this, SymTriggerHistory *newHistRecord) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    newHistRecord->triggerHistoryId = this->sequenceService->nextVal(this->sequenceService, SYM_SEQUENCE_TRIGGER_HIST);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, SymStringUtils_format("%d", newHistRecord->triggerHistoryId));
    args->add(args, newHistRecord->triggerId);
    args->add(args, newHistRecord->sourceTableName);
    args->add(args, SymStringUtils_format("%d", newHistRecord->tableHash));
    args->add(args, newHistRecord->createTime == NULL ? "null" : newHistRecord->createTime->dateTimeString);
    args->add(args, newHistRecord->columnNames);
    args->add(args, newHistRecord->pkColumnNames);
    args->add(args, newHistRecord->lastTriggerBuildReason);
    args->add(args, newHistRecord->nameForDeleteTrigger);
    args->add(args, newHistRecord->nameForInsertTrigger);
    args->add(args, newHistRecord->nameForUpdateTrigger);
    args->add(args, newHistRecord->sourceSchemaName);
    args->add(args, newHistRecord->sourceCatalogName);
    args->add(args, SymStringUtils_format("%d", newHistRecord->triggerRowHash));
    args->add(args, "null"); // getTriggerTemplateHash
    args->add(args, newHistRecord->errorMessage);

    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_TRIGGER_HIST, args, NULL, &error);


}

SymList * SymTriggerRouterService_getTriggersToSync(SymTriggerRouterService *this) {
    char *nodeGroupId = this->parameterService->getNodeGroupId(this->parameterService);
    SymList *triggers = SymList_new(NULL);
    SymList *triggerRouters = SymTriggerRouterService_getTriggerRouters(this);

    int i;
    for (i = 0; i < triggerRouters->size; i++) {
        SymTriggerRouter *triggerRouter = triggerRouters->get(triggerRouters, i);
        unsigned short nodeGroupIdMatches = strcmp(nodeGroupId, triggerRouter->router->nodeGroupLink->sourceNodeGroupId);
        if (nodeGroupIdMatches) {
            triggers->add(triggers, triggerRouter->trigger);
        }
    }

    return triggers;
}

void SymTriggerRouterService_dropTriggers(SymTriggerRouterService *this, SymTriggerHistory *history) {
    if (SymStringUtils_isNotBlank(history->nameForInsertTrigger)) {

    }

    if (SymStringUtils_isNotBlank(history->nameForDeleteTrigger)) {

    }

    if (SymStringUtils_isNotBlank(history->nameForUpdateTrigger)) {

    }

    SymTriggerRouterService_inactivateTriggerHistory(this, history);
}


void SymTriggerRouterService_inactivateTriggers(SymTriggerRouterService *this, SymList *triggersThatShouldBeActive, SymList *activeTriggerHistories) {
    int i;
    for (i = 0; i < activeTriggerHistories->size; i++) {
        SymTriggerHistory *history = activeTriggerHistories->get(activeTriggerHistories, i);
        unsigned int removeTrigger = 1;

        // TODO
//        for (Trigger trigger : triggersThatShouldBeActive) {
//            if (trigger.getTriggerId().equals(history.getTriggerId())) {
//                removeTrigger = false;
//                break;
//            }
//        }

        if (removeTrigger) {
            SymLog_info("About to remove triggers for inactivated table: %s", history);
            SymTriggerRouterService_dropTriggers(this, history);
        }
    }

}

char * SymTriggerRouterService_getTriggerName(SymTriggerRouterService *this, SymDataEventType dml, int maxTriggerNameLength, SymTrigger *trigger, SymTable *table, SymList *activeTriggerHistories) {
    char *triggerName = NULL;

    switch (dml) {
        case SYM_DATA_EVENT_INSERT:
            if (!SymStringUtils_isBlank(trigger->nameForInsertTrigger)) {
                triggerName = trigger->nameForInsertTrigger;
            }
            break;
        case SYM_DATA_EVENT_UPDATE:
            if (!SymStringUtils_isBlank(trigger->nameForUpdateTrigger)) {
                triggerName = trigger->nameForUpdateTrigger;
            }
            break;
        case SYM_DATA_EVENT_DELETE:
            if (!SymStringUtils_isBlank(trigger->nameForDeleteTrigger)) {
                triggerName = trigger->nameForDeleteTrigger;
            }
            break;
        default:
            break;
    }

    if (SymStringUtils_isBlank(triggerName)) {
        char *tablePrefix = ""; // TODO
        char *dmlCode = SymStringUtils_toLowerCase(SymDataEvent_getCode(dml));

        char *triggerPrefix1 = SymStringUtils_format("%s%s", tablePrefix, "_");
        char *triggerSuffix1 = SymStringUtils_format("%s%s%s", "on_", dmlCode, "_for_");
        char *triggerSuffix2 = trigger->triggerId; // TODO replaceCharsToShortenName
// TODO
//        if (trigger.isSourceTableNameWildCarded()) {
//            triggerSuffix2 = replaceCharsToShortenName(table.getName());
//        }

        char *triggerSuffix3 = SymStringUtils_format("%s%s", "_", this->parameterService->getNodeGroupId(this->parameterService)); // TODO replaceCharsToShortenName

        triggerName = SymStringUtils_format("%s%s%s%s",
                triggerPrefix1, triggerSuffix1, triggerSuffix2, triggerSuffix3);

        // TODO check triggerName max length.

    }

    triggerName = SymStringUtils_toUpperCase(triggerName);

    // TODO check triggerName max length.
    // TODO check triggerName in use.

    return triggerName;
}

SymTriggerHistory * SymTriggerRouterService_rebuildTriggerIfNecessary(SymTriggerRouterService *this, unsigned short forceRebuild, SymTrigger *trigger, SymDataEventType dmlType, char *reason, SymTriggerHistory *oldhist, SymTriggerHistory *hist, unsigned short triggerIsActive, SymTable *table, SymList *activeTriggerHistories) {
    unsigned short triggerExists = 0;
    unsigned short triggerRemoved = 0;

    // TODO create the SymTriggerHistory
    SymTriggerHistory *newTriggerHist = SymTriggerHistory_new(NULL);

    newTriggerHist->triggerId = trigger->triggerId;
    newTriggerHist->lastTriggerBuildReason = reason;
    newTriggerHist->sourceTableName = trigger->sourceTableName; // TODO trigger.isSourceTableNameWildCarded()

    newTriggerHist->columnNames = SymTable_getCommaDeliminatedColumns(trigger->orderColumnsForTable(trigger, table));
    newTriggerHist->pkColumnNames = SymTable_getCommaDeliminatedColumns(trigger->getSyncKeysColumnsForTable(trigger, table));
    newTriggerHist->triggerRowHash = trigger->toHashedValue(trigger);
   // newTriggerHist->triggerTemplateHash = TODO
    newTriggerHist->tableHash = table->calculateTableHashcode(table);

    int maxTriggerNameLength = 50; // TODO

    if (trigger->syncOnInsert) {
        char* triggerName = SymTriggerRouterService_getTriggerName(this, SYM_DATA_EVENT_INSERT, maxTriggerNameLength, trigger, table, activeTriggerHistories);
        newTriggerHist->nameForInsertTrigger = SymStringUtils_toUpperCase(triggerName);
    }
    if (trigger->syncOnUpdate) {
        char* triggerName = SymTriggerRouterService_getTriggerName(this, SYM_DATA_EVENT_UPDATE, maxTriggerNameLength, trigger, table, activeTriggerHistories);
        newTriggerHist->nameForUpdateTrigger = SymStringUtils_toUpperCase(triggerName);
    }
    if (trigger->syncOnDelete) {
        char* triggerName = SymTriggerRouterService_getTriggerName(this, SYM_DATA_EVENT_DELETE, maxTriggerNameLength, trigger, table, activeTriggerHistories);
        newTriggerHist->nameForDeleteTrigger = SymStringUtils_toUpperCase(triggerName);
    }

    // TODO figure out old trigger stuff.
    char *oldTriggerName = NULL;
    char *oldSourceSchema = NULL;
    char *oldCatalogName = NULL;

    if (oldhist != NULL) {
        oldTriggerName = oldhist->getTriggerNameForDmlType(oldhist, dmlType);
        oldSourceSchema = oldhist->sourceSchemaName;
        oldCatalogName = oldhist->sourceCatalogName;
        triggerExists = this->symmetricDialect->doesTriggerExist(this->symmetricDialect,
                oldCatalogName, oldSourceSchema, oldhist->sourceTableName, oldTriggerName);
    } else {
        // We had no trigger_hist row, lets validate that the trigger as
        // defined in the trigger row data does not exist as well.
        oldTriggerName = newTriggerHist->getTriggerNameForDmlType(newTriggerHist, dmlType);
        oldSourceSchema = table->schema;
        oldCatalogName = table->catalog;
        if (SymStringUtils_isNotBlank(oldTriggerName)) {
            triggerExists = this->symmetricDialect->doesTriggerExist(this->symmetricDialect,
                    oldCatalogName, oldSourceSchema, table->name, oldTriggerName);
        }
    }

    if (!triggerExists && forceRebuild) {
        reason = SYM_TRIGGER_REBUILD_REASON_TRIGGERS_MISSING;
    }

    if ((forceRebuild || !triggerIsActive) && triggerExists) {
        this->symmetricDialect->removeTrigger(this->symmetricDialect,
                           NULL, oldCatalogName, oldSourceSchema, table->name, oldTriggerName);
        triggerExists = 0;
        triggerRemoved = 1;
    }

    unsigned short isDeadTrigger = !trigger->syncOnInsert && !trigger->syncOnUpdate && !trigger->syncOnDelete;

    if (hist == NULL && (oldhist == NULL || (!triggerExists && triggerIsActive) || (isDeadTrigger && forceRebuild))) {
        SymTriggerRouterService_insert(this, newTriggerHist);
        // TODO
        hist = newTriggerHist;
    }

    if (!triggerExists && triggerIsActive) {
        SymChannel *channel = this->configurationService->getChannel(this->configurationService, trigger->channelId);
        this->symmetricDialect->createTrigger(this->symmetricDialect, dmlType, trigger, hist, channel, NULL, table);
    }

    return hist;
}


void SymTriggerRouterService_updateOrCreateDatabaseTriggers(SymTriggerRouterService *this, SymTrigger *trigger, SymTable *table, SymList *activeTriggerHistories, unsigned short force) {
    SymTriggerHistory *newestHistory;
    char* reason = SYM_TRIGGER_REBUILD_REASON_NEW_TRIGGERS;

    unsigned short foundPk = 0;
    // TODO filder excluded columns.
    SymList *columns = table->columns;

    int i = 0;
    for (i = 0; i < columns->size; i++) {
        SymColumn *column = columns->get(columns, i);
        foundPk |= column->isPrimaryKey;
    }

    if (!foundPk) {
        // TODO
        // table = platform.makeAllColumnsPrimaryKeys(table);
    }

    SymTriggerHistory *latestHistoryBeforeRebuild = NULL; // TODO

    unsigned short forceRebuildOfTriggers = 0;

    if (latestHistoryBeforeRebuild == NULL) {
        reason = SYM_TRIGGER_REBUILD_REASON_NEW_TRIGGERS;
        forceRebuildOfTriggers = 1;
    }
    else {
        // TODO support replacing existing triggers...
    }

    unsigned short supportsTriggers = 1; // TODO

    newestHistory = SymTriggerRouterService_rebuildTriggerIfNecessary(this, forceRebuildOfTriggers,
            trigger, SYM_DATA_EVENT_INSERT, reason, latestHistoryBeforeRebuild, NULL,
            trigger->syncOnInsert && supportsTriggers, table, activeTriggerHistories);

//    newestHistory = SymTriggerRouterService_rebuildTriggerIfNecessary(this, forceRebuildOfTriggers,
//            trigger, SYM_DATA_EVENT_UPDATE, reason, latestHistoryBeforeRebuild, newestHistory,
//            trigger->syncOnInsert && supportsTriggers, table, activeTriggerHistories);
//
//    newestHistory = SymTriggerRouterService_rebuildTriggerIfNecessary(this, forceRebuildOfTriggers,
//            trigger, SYM_DATA_EVENT_DELETE, reason, latestHistoryBeforeRebuild, newestHistory,
//            trigger->syncOnInsert && supportsTriggers, table, activeTriggerHistories);

    if (latestHistoryBeforeRebuild != NULL && newestHistory != NULL) {
        SymTriggerRouterService_inactivateTriggerHistory(this, latestHistoryBeforeRebuild);
    }

}

char * SymTriggerRouterService_replaceCharsToShortenName(SymTriggerRouterService *this, char *triggerName) {
    // TODO
    return 0;
}

SymNodeGroupLink * SymRouterMapper_getNodeGroupLink(SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId) {
    // TODO
    return 0;
}

void SymTriggerRouterService_syncTriggers(SymTriggerRouterService *this, unsigned short force) {
    unsigned short autoSyncTriggers = this->parameterService->is(this->parameterService, AUTO_SYNC_TRIGGERS, 1);

    if (autoSyncTriggers) {
        SymLog_info("Synchronizing triggers");
        SymList *triggers = SymTriggerRouterService_getTriggersToSync(this);
        SymList *activeTriggerHistories = SymTriggerRouterService_getActiveTriggerHistories(this);
        SymTriggerRouterService_inactivateTriggers(this, triggers, activeTriggerHistories);
        int i;
        for (i = 0; i < triggers->size; i++) {
            SymTrigger *trigger = triggers->get(triggers, i);
            SymTable *table = this->platform->getTableFromCache(this->platform,
                    trigger->sourceCatalogName, trigger->sourceSchemaName, trigger->sourceTableName, 1);
            if (table) {
                SymTriggerRouterService_updateOrCreateDatabaseTriggers(this, trigger, table, activeTriggerHistories, force);
            }
            else {
                SymLog_error("No table '%s' found for trigger. ", trigger->sourceTableName);
            }
        }
    }
}

void SymTriggerRouterService_destroy(SymTriggerRouterService * this) {
    this->historyMap->destroy(this->historyMap);
    this->routersCache->destroy(this->routersCache);
    free(this);
}

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService *this,
        SymConfigurationService *configurationService, SymSequenceService *sequenceService,
        SymParameterService *parameterService, SymDatabasePlatform *platform, SymDialect *symmetricDialect) {

    if (this == NULL) {
        this = (SymTriggerRouterService*) calloc(1, sizeof(SymTriggerRouterService));
    }

    this->historyMap = SymMap_new(NULL, 100);
    this->routersCache = SymMap_new(NULL, 10);

    this->configurationService = configurationService;
    this->sequenceService = sequenceService;
    this->parameterService = parameterService;
    this->symmetricDialect = symmetricDialect;
    this->platform = platform;

    this->syncTriggers = (void *) &SymTriggerRouterService_syncTriggers;
    this->getTriggerHistory = (void *) &SymTriggerRouterService_getTriggerHistory;
    this->getActiveTriggerHistories = (void *) &SymTriggerRouterService_getActiveTriggerHistories;
    this->getActiveTriggerHistoriesByTrigger = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesByTrigger;
    this->getActiveTriggerHistoriesByTableName = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesByTableName;
    this->getRouters = (void *) &SymTriggerRouterService_getRouters;
    this->getRouterById = (void *) &SymTriggerRouterService_getRouterById;
    this->destroy = (void *) &SymTriggerRouterService_destroy;
    return this;
}
