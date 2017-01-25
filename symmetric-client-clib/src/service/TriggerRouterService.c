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
    long routerCacheTimeoutInMs = this->parameterService->getLong(this->parameterService, SYM_PARAMETER_CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS, 600000);
    if (this->routersCache == NULL || refreshCache || time(NULL) * 1000 - this->routersCacheTime * 1000 > routerCacheTimeoutInMs) {
        if (this->routersCache) {
            this->routersCache->destroy(this->routersCache);
        }
        this->routersCache = SymMap_new(NULL, 10);
        this->routersCacheTime = time(NULL);
        SymList *routers = this->getRouters(this, 1);
        SymIterator *iter = routers->iterator(routers);
        while (iter->hasNext(iter)) {
            SymRouter *router = iter->next(iter);
            this->routersCache->put(this->routersCache, router->routerId, router);
        }
        iter->destroy(iter);
    }
    return (SymRouter *) this->routersCache->get(this->routersCache, routerId);
}

SymList * SymTriggerRouterService_getRouters(SymTriggerRouterService *this, unsigned short replaceVariables) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sb = SymStringBuilder_newWithString("select ");
    sb->append(sb, SYM_SQL_SELECT_ROUTERS_COLUMN_LIST)->append(sb, SYM_SQL_SELECT_ROUTERS);
    int error;
    SymList *routers = sqlTemplate->query(sqlTemplate, sb->str, NULL, NULL, &error, (void *) SymTriggerRouterService_routerMapper);
    sb->destroy(sb);
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
        this->historyMap->putByInt(this->historyMap, histId, history);
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

    if (this->historyMap->size > 0) {
        this->historyMap->resetAll(this->historyMap, (void *)SymTriggerHistory_destroy);
    }

    SymIterator *iter = histories->iterator(histories);
    while (iter->hasNext(iter)) {
        SymTriggerHistory *triggerHistory = (SymTriggerHistory *) iter->next(iter);
        this->historyMap->putByInt(this->historyMap, triggerHistory->triggerHistoryId, triggerHistory);
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

static SymTrigger * buildTriggerForSymmetricTable(SymTriggerRouterService *this, char *tableName) {
    SymList *tablesThatDoNotSync = SymTableConstants_getTablesThatDoNotSync();
    unsigned short syncChanges = !tablesThatDoNotSync->contains(tablesThatDoNotSync, tableName, (void *) strcmp)
            && this->parameterService->is(this->parameterService, SYM_PARAMETER_AUTO_SYNC_CONFIGURATION, 1);
    unsigned short syncOnIncoming = this->parameterService->is(this->parameterService, SYM_PARAMETER_AUTO_SYNC_CONFIGURATION_ON_INCOMING, 1)
            || SymStringUtils_equals(tableName, SYM_TABLE_RELOAD_REQUEST);
    SymTrigger *trigger = SymTrigger_new(NULL);
    trigger->triggerId = tableName;
    trigger->syncOnDelete = syncChanges;
    trigger->syncOnInsert = syncChanges;
    trigger->syncOnUpdate = syncChanges;
    trigger->syncOnIncomingBatch = syncOnIncoming;
    trigger->sourceTableName = tableName;
    trigger->useCaptureOldData = 0;
    if (SymStringUtils_equals(tableName, SYM_NODE_HOST)) {
        trigger->channelId = SYM_CHANNEL_HEARTBEAT;
    } else if (SymStringUtils_equals(tableName, SYM_FILE_SNAPSHOT)) {
        trigger->channelId = SYM_CHANNEL_FILESYNC;
        trigger->reloadChannelId = SYM_CHANNEL_FILESYNC_RELOAD;
        trigger->useCaptureOldData = 1;
        unsigned short syncEnabled = this->parameterService->is(this->parameterService, SYM_PARAMETER_FILE_SYNC_ENABLE, 0);
        trigger->syncOnInsert = syncEnabled;
        trigger->syncOnUpdate = syncEnabled;
        trigger->syncOnDelete = 0;
    }

    else {
        trigger->channelId = SYM_CHANNEL_CONFIG;
    }

    if (!SymStringUtils_equals(tableName, SYM_NODE_HOST) && !SymStringUtils_equals(tableName, SYM_NODE) &&
            !SymStringUtils_equals(tableName, SYM_NODE_SECURITY) && !SymStringUtils_equals(tableName, SYM_TABLE_RELOAD_REQUEST)) {
        trigger->useCaptureLobs = 1;
    }
    trigger->lastUpdateTime = SymDate_new();
    tablesThatDoNotSync->destroy(tablesThatDoNotSync);
    return trigger;
}

static SymList * buildTriggersForSymmetricTables(SymTriggerRouterService *this, SymList *tablesToExclude) {
    SymList *tables = SymTableConstants_getConfigTables();

    SymList *definedTriggers = this->getTriggers(this, 1);
    SymIterator *iter = definedTriggers->iterator(definedTriggers);
    while (iter->hasNext(iter)) {
        SymTrigger *trigger = (SymTrigger *) iter->next(iter);
        tables->removeObject(tables, trigger->sourceTableName, (void *) strcmp);
    }
    iter->destroy(iter);

    if (tablesToExclude) {
        iter = tablesToExclude->iterator(tablesToExclude);
        while (iter->hasNext(iter)) {
            char *tableName = (char *) iter->next(iter);
            tables->removeObject(tables, tableName, (void *) strcasecmp);
        }
        iter->destroy(iter);
    }

    SymList *triggers = SymList_new(NULL);
    iter = tables->iterator(tables);
    while (iter->hasNext(iter)) {
        char *tableName = (char *) iter->next(iter);
        SymTrigger *trigger = buildTriggerForSymmetricTable(this, tableName);
        triggers->add(triggers, trigger);
    }

    definedTriggers->destroyAll(definedTriggers, (void *)SymTrigger_destroy);

    tables->destroy(tables);
    iter->destroy(iter);
    return triggers;
}

SymTrigger * SymTriggerRouterService_getTriggerById(SymTriggerRouterService *this, char *triggerId, unsigned short refreshCache) {
    long triggerCacheTimeoutInMs = this->parameterService->getLong(this->parameterService, SYM_PARAMETER_CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS, 600000);
    if (this->triggersCache == NULL || refreshCache || time(NULL) * 1000 - this->triggersCacheTime * 1000 > triggerCacheTimeoutInMs) {
        if (this->triggersCache) {
            this->triggersCache->destroy(this->triggersCache);
        }
        this->triggersCache = SymMap_new(NULL, 100);
        this->triggersCacheTime = time(NULL);
        SymList *triggers = this->getTriggers(this, 1);
        SymList *symTriggers = buildTriggersForSymmetricTables(this, NULL);
        triggers->addAll(triggers, symTriggers);
        SymIterator *iter = triggers->iterator(triggers);
        while (iter->hasNext(iter)) {
            SymTrigger *trigger = iter->next(iter);
            this->triggersCache->put(this->triggersCache, trigger->triggerId, trigger);
        }
        iter->destroy(iter);
        triggers->destroy(triggers);
        symTriggers->destroy(symTriggers);
    }
    return (SymTrigger *) this->triggersCache->get(this->triggersCache, triggerId);
}

SymList * SymTriggerRouterService_getTriggers(SymTriggerRouterService *this, unsigned short replaceTokens) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    int error;
    SymList* triggers = sqlTemplate->query(sqlTemplate, SYM_SQL_SELECT_TRIGGERS, NULL, NULL, &error, (void *) SymTriggerRouterService_triggerMapper);
    return triggers;
}

void SymTriggerRouterService_inactivateTriggerHistory(SymTriggerRouterService *this, SymTriggerHistory *history) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    char *triggerHistoryId = SymStringUtils_format("%d", history->triggerHistoryId);
    SymStringArray *args = SymStringArray_new(NULL);

    args->add(args, history->errorMessage);
    args->add(args, triggerHistoryId);

    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_INACTIVATE_TRIGGER_HISTORY, args, NULL, &error);

    args->destroy(args);
    free(triggerHistoryId);
}

unsigned short SymTriggerRouterService_isTriggerNameInUse(SymTriggerRouterService *this, SymList *activeTriggerHistories, char *triggerId, char *triggerName) {
    int i;
    for (i = 0; i < activeTriggerHistories->size; ++i) {
        SymTriggerHistory *triggerHistory = activeTriggerHistories->get(activeTriggerHistories, i);
        if (! SymStringUtils_equals(triggerHistory->triggerId, triggerId)
                && (SymStringUtils_equals(triggerHistory->nameForDeleteTrigger, triggerName)
                        || SymStringUtils_equals(triggerHistory->nameForInsertTrigger, triggerName)
                        || SymStringUtils_equals(triggerHistory->nameForUpdateTrigger, triggerName))) {
            return 1;
        }
    }
    return 0;
}

SymTriggerHistory * SymTriggerRouterService_getNewestTriggerHistoryForTrigger(SymTriggerRouterService *this, char *triggerId, char *catalogName, char *schemaName, char *tableName) {
    SymTriggerHistory *result = NULL;

    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, triggerId);
    args->add(args, tableName);

    int error;
    SymList *triggerHistories = sqlTemplate->query(sqlTemplate, SYM_SQL_LATEST_TRIGGER_HIST, args, NULL, &error, (void *) SymTriggerRouterService_triggerHistoryMapper);

    int i;
    for (i = 0; i < triggerHistories->size; ++i) {
        SymTriggerHistory *triggerHistory = triggerHistories->get(triggerHistories, i);

        unsigned short catalogMatches = (SymStringUtils_isBlank(catalogName) && SymStringUtils_isBlank(triggerHistory->sourceCatalogName))
            || (SymStringUtils_isNotBlank(catalogName) && SymStringUtils_equals(catalogName, triggerHistory->sourceCatalogName));
        unsigned short schemaMatches = (SymStringUtils_isBlank(schemaName) && SymStringUtils_isBlank(triggerHistory->sourceSchemaName))
            || (SymStringUtils_isNotBlank(schemaName) && SymStringUtils_equals(schemaName, triggerHistory->sourceSchemaName));

        if (catalogMatches && schemaMatches) {
            result = triggerHistory;
            triggerHistories->remove(triggerHistories, i); // remove so we can safely destroy the rest if the list below.
            break;
        }
    }

    args->destroy(args);
    if (result) {
        triggerHistories->destroyAll(triggerHistories, (void *) result->destroy);
    }


    return result;
}

SymList * SymTriggerRouterService_enhanceTriggerRouters(SymTriggerRouterService *this, SymList *triggerRouters) {
    SymMap *routersById = SymMap_new(NULL, 8);
    SymList *routers = SymTriggerRouterService_getRouters(this, 1);
    int i;
    for (i = 0; i < routers->size; i++) {
        SymRouter *router = routers->get(routers, i);
        char *trimmed = SymStringUtils_trim(router->routerId);
        char *routerId = SymStringUtils_toUpperCase(trimmed);
        routersById->put(routersById, routerId, router);
        free(trimmed);
        free(routerId);
    }

    SymMap *triggersById = SymMap_new(NULL, 8);
    SymList *triggers = SymTriggerRouterService_getTriggers(this, 1);
    for (i = 0; i < triggers->size; i++) {
        SymTrigger *trigger = triggers->get(triggers, i);
        char *trimmed = SymStringUtils_trim(trigger->triggerId);
        char *triggerId = SymStringUtils_toUpperCase(trimmed);
        triggersById->put(triggersById, triggerId, trigger);
        free(trimmed);
        free(triggerId);
    }

    for (i = 0; i < triggerRouters->size; i++) {
        SymTriggerRouter *triggerRouter = triggerRouters->get(triggerRouters, i);
        char *triggerIdTrimmed = SymStringUtils_trim(triggerRouter->trigger->triggerId);
        char *triggerId = SymStringUtils_toUpperCase(triggerIdTrimmed);
        char *routerIdTrimmed = SymStringUtils_trim(triggerRouter->router->routerId);
        char *routerId = SymStringUtils_toUpperCase(routerIdTrimmed);

        triggerRouter->trigger->destroy(triggerRouter->trigger);
        triggerRouter->trigger = triggersById->get(triggersById, triggerId );
        triggerRouter->router->destroy(triggerRouter->router);
        triggerRouter->router = routersById->get(routersById, routerId );

        free(triggerId);
        free(routerId);
        free(triggerIdTrimmed);
        free(routerIdTrimmed);
    }

    triggers->destroy(triggers);
    routers->destroy(routers);
    triggersById->destroy(triggersById);
    routersById->destroy(routersById);

    return triggerRouters;
}

SymList * SymTriggerRouterService_getTriggerRouters(SymTriggerRouterService *this) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sql = SymStringBuilder_newWithString("select ");
    sql->append(sql, SYM_SQL_SELECT_TRIGGER_ROUTERS_COLUMN_LIST);
    sql->append(sql, SYM_SQL_SELECT_TRIGGER_ROUTERS);

    int error;
    SymList* triggerRouters = sqlTemplate->query(sqlTemplate, sql->str, NULL, NULL, &error, (void *) SymTriggerRouterService_triggerRouterMapper);
    triggerRouters = SymTriggerRouterService_enhanceTriggerRouters(this, triggerRouters);

    sql->destroy(sql);
    return triggerRouters;
}


void SymTriggerRouterService_insert(SymTriggerRouterService *this, SymTriggerHistory *newHistRecord) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    newHistRecord->triggerHistoryId = this->sequenceService->nextVal(this->sequenceService, SYM_SEQUENCE_TRIGGER_HIST);

    char *tableHash = SymStringUtils_format("%d", newHistRecord->tableHash);
    char *triggerRowHash = SymStringUtils_format("%ld", newHistRecord->triggerRowHash);
    char *triggerTemplateHash = SymStringUtils_format("%ld", newHistRecord->triggerTemplateHash);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, newHistRecord->triggerId);
    args->add(args, newHistRecord->sourceTableName);
    args->add(args, tableHash);
    args->add(args, newHistRecord->createTime == NULL ? "null" : newHistRecord->createTime->dateTimeString);
    args->add(args, newHistRecord->columnNames);
    args->add(args, newHistRecord->pkColumnNames);
    args->add(args, newHistRecord->lastTriggerBuildReason);
    args->add(args, newHistRecord->nameForDeleteTrigger);
    args->add(args, newHistRecord->nameForInsertTrigger);
    args->add(args, newHistRecord->nameForUpdateTrigger);
    args->add(args, newHistRecord->sourceSchemaName);
    args->add(args, newHistRecord->sourceCatalogName);
    args->add(args, triggerRowHash);
    args->add(args, triggerTemplateHash);
    args->add(args, newHistRecord->errorMessage);

    int error;
    sqlTemplate->update(sqlTemplate, SYM_SQL_INSERT_TRIGGER_HIST, args, NULL, &error);

    free(tableHash);
    free(triggerRowHash);
    free(triggerTemplateHash);
    args->destroy(args);

}

//void SymTriggerRouterService_destroyTriggerRouters(SymTriggerRouterService *this, SymList *triggerRouters) {
//    // Cleanup discarded TriggerRouters completely. Need to keep track of the
//    // freedRouters and Triggers because there are multiple pointers to the same
//    // memory location within these objects and we can't free the same pointer
//    // twice.
//    SymList *freedRouters = SymList_new(NULL);
//    SymList *freedTriggers = SymList_new(NULL);
//
//    int i;
//    for (i = 0; i < triggerRouters->size; i++) {
//        SymTriggerRouter *triggerRouter = triggerRouters->get(triggerRouters, i);
//        unsigned short shouldFreeRouter = 1;
//        int j =0;
//        for (j =0; j < freedRouters->size; j++) {
//            if (freedRouters->get(freedRouters, j) == triggerRouter->router) {
//                shouldFreeRouter = 0;
//                break; //already freed.
//            }
//        }
//
//        if (shouldFreeRouter && triggerRouter->router) {
//            free(triggerRouter->router->routerId);
//            triggerRouter->router->destroy(triggerRouter->router);
//            freedRouters->add(freedRouters, triggerRouter->router);
//            triggerRouter->router = NULL;
//        }
//        unsigned short shouldFreeTrigger = 1;
//
//        for (j =0; j < freedTriggers->size; j++) {
//            if (freedTriggers->get(freedTriggers, j) == triggerRouter->trigger) {
//                shouldFreeTrigger = 0;
//                break; //already freed.
//            }
//        }
//
//        if (shouldFreeTrigger && triggerRouter->trigger) {
//            triggerRouter->trigger->destroy(triggerRouter->trigger);
//            freedTriggers->add(freedRouters, triggerRouter->trigger);
//            triggerRouter->trigger = NULL;
//        }
//    }
//
//    triggerRouters->destroyAll(triggerRouters, (void *)SymTriggerRouter_destroy);
//    freedRouters->destroy(freedRouters);
//    freedTriggers->destroy(freedTriggers);
//}

SymList * SymTriggerRouterService_getTriggersToSync(SymTriggerRouterService *this) {
    char *nodeGroupId = this->parameterService->getNodeGroupId(this->parameterService);
    SymList *triggers = SymList_new(NULL);
    SymList *triggerRouters = SymTriggerRouterService_getTriggerRouters(this);

    int i;
    for (i = 0; i < triggerRouters->size; i++) {
        SymTriggerRouter *triggerRouter = triggerRouters->get(triggerRouters, i);
        unsigned short nodeGroupIdMatches = SymStringUtils_equals(nodeGroupId, triggerRouter->router->nodeGroupLink->sourceNodeGroupId);
        if (nodeGroupIdMatches) {
            triggers->add(triggers, triggerRouter->trigger);
            // ultimately we just wanted the trigger from the TriggerRouter.
            // So break the link to the trigger here so we can destroy the rest of
            // the objects below.
            triggerRouter->trigger = NULL;
        }
    }

    // SymTriggerRouterService_destroyTriggerRouters(this, triggerRouters);

    return triggers;
}

void SymTriggerRouterService_dropTriggers(SymTriggerRouterService *this, SymTriggerHistory *history) {
    if (SymStringUtils_isNotBlank(history->nameForInsertTrigger)) {
        this->symmetricDialect->removeTrigger(this->symmetricDialect, NULL, history->sourceCatalogName,
                history->sourceSchemaName, history->nameForInsertTrigger, history->sourceTableName);
    }

    if (SymStringUtils_isNotBlank(history->nameForDeleteTrigger)) {
        this->symmetricDialect->removeTrigger(this->symmetricDialect, NULL, history->sourceCatalogName,
                history->sourceSchemaName, history->nameForDeleteTrigger, history->sourceTableName);
    }

    if (SymStringUtils_isNotBlank(history->nameForUpdateTrigger)) {
        this->symmetricDialect->removeTrigger(this->symmetricDialect, NULL, history->sourceCatalogName,
                history->sourceSchemaName, history->nameForUpdateTrigger, history->sourceTableName);
    }

    SymTriggerRouterService_inactivateTriggerHistory(this, history);
}


void SymTriggerRouterService_inactivateTriggers(SymTriggerRouterService *this, SymList *triggersThatShouldBeActive, SymList *activeTriggerHistories) {
    int i;
    for (i = 0; i < activeTriggerHistories->size; i++) {
        SymTriggerHistory *history = activeTriggerHistories->get(activeTriggerHistories, i);
        unsigned int removeTrigger = 1;

        int j;
        for (j = 0; j < triggersThatShouldBeActive->size; j++) {
            SymTrigger *trigger = triggersThatShouldBeActive->get(triggersThatShouldBeActive, j);
            if (SymStringUtils_equals(trigger->triggerId, history->triggerId)) {
                removeTrigger = 0;
                break;
            }
        }

        if (removeTrigger) {
            SymLog_info("About to remove triggers for inactivated table: %s", history);
            SymTriggerRouterService_dropTriggers(this, history);
        }
    }

}

char * SymTriggerRouterService_replaceCharsToShortenName(SymTriggerRouterService *this, char *triggerName) {
    int triggerNameSize = strlen(triggerName);
    SymStringBuilder *buff = SymStringBuilder_newWithSize(triggerNameSize);

    int i;
    for (i = 0; i < triggerNameSize; ++i) {
        char c = triggerName[i];
        if (((c >= 'a' && c <= 'z') || (c >= 'A' && c <= 'Z') || (c >= '0' && c <= '9') || (c == '_'))) {
            if (c != 'a' && c != 'e' && c != 'i' && c != 'o' && c != 'u'
                    && c != 'A' && c != 'E' && c != 'I' && c != 'O' && c != 'U') {
                buff->appendf(buff, "%c", c);
            }
        }
    }

    return buff->destroyAndReturn(buff);
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
        char *tablePrefix = "sym";
        char *dmlCode = SymStringUtils_toLowerCase(SymDataEvent_getCode(dml));

        char *triggerPrefix1 = SymStringUtils_format("%s%s", tablePrefix, "_");
        char *triggerSuffix1 = SymStringUtils_format("%s%s%s", "on_", dmlCode, "_for_");
        char *triggerSuffix2 = SymTriggerRouterService_replaceCharsToShortenName(this, trigger->triggerId);


// TODO
//        if (trigger.isSourceTableNameWildCarded()) {
//            triggerSuffix2 = replaceCharsToShortenName(table.getName());
//        }

        char* triggerSuffix3Temp = SymStringUtils_format("%s%s", "_", this->parameterService->getNodeGroupId(this->parameterService));
        char *triggerSuffix3 = SymTriggerRouterService_replaceCharsToShortenName(this, triggerSuffix3Temp);
        free(triggerSuffix3Temp);

        triggerName = SymStringUtils_format("%s%s%s%s",
                triggerPrefix1, triggerSuffix1, triggerSuffix2, triggerSuffix3);

        if (strlen(triggerName) > maxTriggerNameLength && maxTriggerNameLength > 0) {
            char *oldTriggerName = triggerName;
            triggerName = SymStringUtils_format("%s%s%s",
                    triggerPrefix1, triggerSuffix1, triggerSuffix2);
            free(oldTriggerName);
        }

        free(dmlCode);
        free(triggerPrefix1);
        free(triggerSuffix1);
        free(triggerSuffix2);
        free(triggerSuffix3);
    }

    char *oldTriggerName = triggerName;
    triggerName = SymStringUtils_toUpperCase(triggerName);
    free(oldTriggerName);

    if (strlen(triggerName) > maxTriggerNameLength && maxTriggerNameLength > 0) {
        char *oldTriggerName = triggerName;
        triggerName = SymStringUtils_substring(triggerName, 0, maxTriggerNameLength - 1);
        free(oldTriggerName);
        SymLog_debug("We just truncated the trigger name for the %s trigger id=%s.  You might want to consider manually providing a name for the trigger that is less than %d characters long",
                SymDataEvent_getCode(dml), trigger->triggerId, maxTriggerNameLength);
    }

    int duplicateCount = 0;
    while (SymTriggerRouterService_isTriggerNameInUse(this, activeTriggerHistories, trigger->triggerId, triggerName)) {
        duplicateCount++;
        char *duplicateSuffix = SymStringUtils_format("%d", duplicateCount);
        if (strlen(triggerName) + strlen(duplicateSuffix) > maxTriggerNameLength) {
            char *shortenedTriggerName = SymStringUtils_substring(triggerName, 0, strlen(triggerName)-strlen(duplicateSuffix));
            char *oldTriggerName = triggerName;
            triggerName = SymStringUtils_format("%s%s", shortenedTriggerName, duplicateSuffix);

            free(oldTriggerName);
            free(shortenedTriggerName);
        }
        else {
            triggerName = SymStringUtils_format("%s%s", triggerName, duplicateSuffix);
        }
        free(duplicateSuffix);
    }

    return triggerName;
}

SymTriggerHistory * SymTriggerRouterService_rebuildTriggerIfNecessary(SymTriggerRouterService *this, unsigned short forceRebuild, SymTrigger *trigger, SymDataEventType dmlType, char *reason, SymTriggerHistory *oldhist, SymTriggerHistory *hist, unsigned short triggerIsActive, SymTable *table, SymList *activeTriggerHistories) {
    unsigned short triggerExists = 0;

    SymTriggerHistory *newTriggerHist = SymTriggerHistory_new(NULL);

    newTriggerHist->triggerId = trigger->triggerId;
    newTriggerHist->lastTriggerBuildReason = reason;
    newTriggerHist->sourceTableName = trigger->sourceTableName; // TODO trigger.isSourceTableNameWildCarded()
    SymList *orderColumnsForTable = trigger->orderColumnsForTable(trigger, table);
    newTriggerHist->columnNames = SymTable_getCommaDeliminatedColumns(orderColumnsForTable);
    orderColumnsForTable->destroy(orderColumnsForTable);

    SymList *syncKeysColumnsForTable = trigger->getSyncKeysColumnsForTable(trigger, table);
    newTriggerHist->pkColumnNames = SymTable_getCommaDeliminatedColumns(syncKeysColumnsForTable);
    syncKeysColumnsForTable->destroy(syncKeysColumnsForTable);

    newTriggerHist->triggerRowHash = trigger->toHashedValue(trigger);
    newTriggerHist->triggerTemplateHash = this->symmetricDialect->triggerTemplate->
            toHashedValue(this->symmetricDialect->triggerTemplate);
    newTriggerHist->tableHash = table->calculateTableHashcode(table);

    int maxTriggerNameLength = 50; // TODO

    if (trigger->syncOnInsert) {
        char* triggerName = SymTriggerRouterService_getTriggerName(this, SYM_DATA_EVENT_INSERT, maxTriggerNameLength, trigger, table, activeTriggerHistories);
        newTriggerHist->nameForInsertTrigger = SymStringUtils_toUpperCase(triggerName);
        free(triggerName);
    }
    if (trigger->syncOnUpdate) {
        char* triggerName = SymTriggerRouterService_getTriggerName(this, SYM_DATA_EVENT_UPDATE, maxTriggerNameLength, trigger, table, activeTriggerHistories);
        newTriggerHist->nameForUpdateTrigger = SymStringUtils_toUpperCase(triggerName);
        free(triggerName);
    }
    if (trigger->syncOnDelete) {
        char* triggerName = SymTriggerRouterService_getTriggerName(this, SYM_DATA_EVENT_DELETE, maxTriggerNameLength, trigger, table, activeTriggerHistories);
        newTriggerHist->nameForDeleteTrigger = SymStringUtils_toUpperCase(triggerName);
        free(triggerName);
    }

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
    }

    unsigned short isDeadTrigger = !trigger->syncOnInsert && !trigger->syncOnUpdate && !trigger->syncOnDelete;

    if (hist == NULL && (oldhist == NULL || (!triggerExists && triggerIsActive) || (isDeadTrigger && forceRebuild))) {
        SymTriggerRouterService_insert(this, newTriggerHist);
        hist = SymTriggerRouterService_getNewestTriggerHistoryForTrigger(this, trigger->triggerId,
                table->catalog, table->schema, table->name);
    }

    if (!triggerExists && triggerIsActive) {
        SymChannel *channel = this->configurationService->getChannel(this->configurationService, trigger->channelId);
        this->symmetricDialect->createTrigger(this->symmetricDialect, dmlType, trigger, hist, channel, NULL, table);
        //channel->destroy(channel);
    }

    newTriggerHist->destroy(newTriggerHist);

    return hist;
}


void SymTriggerRouterService_updateOrCreateDatabaseTriggers(SymTriggerRouterService *this, SymTrigger *trigger, SymTable *table, SymList *activeTriggerHistories, unsigned short force) {
    SymTriggerHistory *newestHistory;
    char* reason = SYM_TRIGGER_REBUILD_REASON_NEW_TRIGGERS;

    unsigned short foundPk = 0;
    // TODO filter excluded columns.
    SymList *columns = table->columns;

    int i = 0;
    for (i = 0; i < columns->size; i++) {
        SymColumn *column = columns->get(columns, i);
        foundPk |= column->isPrimaryKey;
    }

    if (!foundPk) {
        table = this->platform->makeAllColumnsPrimaryKeys(this->platform, table);
    }

    SymTriggerHistory *latestHistoryBeforeRebuild =
            SymTriggerRouterService_getNewestTriggerHistoryForTrigger(this,
                    trigger->triggerId, table->catalog, table->schema, table->name );

    unsigned short forceRebuildOfTriggers = 0;

    long currentTriggerTemplateHash = this->symmetricDialect->triggerTemplate->
            toHashedValue(this->symmetricDialect->triggerTemplate);

    if (latestHistoryBeforeRebuild == NULL) {
        reason = SYM_TRIGGER_REBUILD_REASON_NEW_TRIGGERS;
        forceRebuildOfTriggers = 1;
    } else if (table->calculateTableHashcode(table) != latestHistoryBeforeRebuild->tableHash) {
        reason = SYM_TRIGGER_REBUILD_REASON_TABLE_SCHEMA_CHANGED;
        forceRebuildOfTriggers = 1;
    } else if (trigger->hasChangedSinceLastTriggerBuild(trigger, latestHistoryBeforeRebuild->createTime)
            || trigger->toHashedValue(trigger) != latestHistoryBeforeRebuild->triggerRowHash) {

        reason = SYM_TRIGGER_REBUILD_REASON_TABLE_SYNC_CONFIGURATION_CHANGED;
        forceRebuildOfTriggers = 1;
    } else if (currentTriggerTemplateHash != latestHistoryBeforeRebuild->triggerTemplateHash) {
        reason = SYM_TRIGGER_REBUILD_REASON_TRIGGER_TEMPLATE_CHANGED;
        forceRebuildOfTriggers = 1;
    }
    else if (force) {
        reason = SYM_TRIGGER_REBUILD_REASON_FORCED;
        forceRebuildOfTriggers = 1;
    }

    unsigned short supportsTriggers = 1;

    newestHistory = SymTriggerRouterService_rebuildTriggerIfNecessary(this, forceRebuildOfTriggers,
            trigger, SYM_DATA_EVENT_INSERT, reason, latestHistoryBeforeRebuild, NULL,
            trigger->syncOnInsert && supportsTriggers, table, activeTriggerHistories);

    newestHistory = SymTriggerRouterService_rebuildTriggerIfNecessary(this, forceRebuildOfTriggers,
            trigger, SYM_DATA_EVENT_UPDATE, reason, latestHistoryBeforeRebuild, newestHistory,
            trigger->syncOnUpdate && supportsTriggers, table, activeTriggerHistories);

    newestHistory = SymTriggerRouterService_rebuildTriggerIfNecessary(this, forceRebuildOfTriggers,
            trigger, SYM_DATA_EVENT_DELETE, reason, latestHistoryBeforeRebuild, newestHistory,
            trigger->syncOnDelete && supportsTriggers, table, activeTriggerHistories);

    if (latestHistoryBeforeRebuild != NULL && newestHistory != NULL) {
        SymTriggerRouterService_inactivateTriggerHistory(this, latestHistoryBeforeRebuild);
    }

    if (latestHistoryBeforeRebuild != NULL) {
        latestHistoryBeforeRebuild->destroy(latestHistoryBeforeRebuild);
    }
    if (newestHistory != NULL) {
        newestHistory->destroy(newestHistory);
    }
}

SymNodeGroupLink * SymRouterMapper_getNodeGroupLink(SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId) {
    // TODO
    return 0;
}

void SymTriggerRouterService_syncTriggers(SymTriggerRouterService *this, unsigned short force) {
    unsigned short autoSyncTriggers = this->parameterService->is(this->parameterService, SYM_PARAMETER_AUTO_SYNC_TRIGGERS, 1);

    if (autoSyncTriggers) {
        SymLog_info("Synchronizing triggers");
        SymList *triggers = SymTriggerRouterService_getTriggersToSync(this);
        SymList *symmetricTableTriggers = buildTriggersForSymmetricTables(this, NULL);
        triggers->addAll(triggers, symmetricTableTriggers);

        SymList *activeTriggerHistories = SymTriggerRouterService_getActiveTriggerHistories(this);
        SymTriggerRouterService_inactivateTriggers(this, triggers, activeTriggerHistories);
        int i;
        for (i = 0; i < triggers->size; i++) {
            SymTrigger *trigger = triggers->get(triggers, i);
            SymTable *table = this->platform->getTableFromCache(this->platform,
                    trigger->sourceCatalogName, trigger->sourceSchemaName, trigger->sourceTableName, 1);
            if (table) {
                SymTriggerRouterService_updateOrCreateDatabaseTriggers(this, trigger, table, activeTriggerHistories, force);
                table->destroy(table); // TODO should not do this when the the cache is fully implemented.
            }
            else {
                SymLog_error("No table '%s' found for trigger. ", trigger->sourceTableName);
            }
        }

        symmetricTableTriggers->destroy(symmetricTableTriggers); // shallow destroy here because these objects are also in 'triggers'.
        triggers->destroyAll(triggers, (void *)SymTrigger_destroy);
        activeTriggerHistories->destroy(activeTriggerHistories); // shallow destroy, these trigger histories are used in the cache.
    }
}

static unsigned short SymTriggerRouterService_doesTriggerRouterExistInList(SymList *triggerRouters, SymTriggerRouter *triggerRouter) {
    SymIterator *iter = triggerRouters->iterator(triggerRouters);
    while (iter->hasNext(iter)) {
        SymTriggerRouter *checkMe = (SymTriggerRouter *) iter->next(iter);
        if (checkMe->isSame(checkMe, triggerRouter)) {
            return 1;
        }
    }
    iter->destroy(iter);
    return 0;
}

static char * SymTriggerRouterService_buildSymmetricTableRouterId(SymTriggerRouterService *this, char *triggerId, char *sourceNodeGroupId,
        char *targetNodeGroupId) {
    char *format = SymStringUtils_format("%s_%s_2_%s", triggerId, sourceNodeGroupId, targetNodeGroupId);
    char *symmetricTableRouterId = SymTriggerRouterService_replaceCharsToShortenName(this, format);
    free(format);
    return symmetricTableRouterId;
}

static SymTriggerRouter * SymTriggerRouterService_buildTriggerRoutersForSymmetricTablesWithTrigger(SymTriggerRouterService *this, SymTrigger *trigger,
        SymNodeGroupLink *nodeGroupLink) {
    SymTriggerRouter *triggerRouter = SymTriggerRouter_new(NULL);
    triggerRouter->trigger = trigger;

    SymRouter *router = SymRouter_new(NULL);
    triggerRouter->router = router;
    router->routerId = SymTriggerRouterService_buildSymmetricTableRouterId(this, trigger->triggerId, nodeGroupLink->sourceNodeGroupId,
            nodeGroupLink->targetNodeGroupId);
    router->routerType = SYM_CONFIGURATION_CHANGED_DATA_ROUTER_ROUTER_TYPE;
    router->nodeGroupLink = nodeGroupLink;
    if (trigger->lastUpdateTime) {
        router->lastUpdateTime = SymDate_newWithTime(trigger->lastUpdateTime->time); // Everyone needs their own copy to avoid attempts to free the same memory later.
        triggerRouter->lastUpdateTime = SymDate_newWithTime(trigger->lastUpdateTime->time);
    }


    return triggerRouter;
}

static SymList * SymTriggerRouterService_buildTriggerRoutersForSymmetricTables(SymTriggerRouterService *this, SymNodeGroupLink *nodeGroupLink,
        SymList *tablesToExclude) {
    int initialLoadOrder = 1;

    SymList *triggers = buildTriggersForSymmetricTables(this, tablesToExclude);
    SymList *triggerRouters = SymList_new(NULL);

    SymIterator *iter = triggers->iterator(triggers);
    while (iter->hasNext(iter)) {
        SymTrigger *trigger = (SymTrigger *) iter->next(iter);
        SymTriggerRouter *triggerRouter = SymTriggerRouterService_buildTriggerRoutersForSymmetricTablesWithTrigger(this, trigger, nodeGroupLink);
        triggerRouter->initialLoadOrder = initialLoadOrder++;
        triggerRouters->add(triggerRouters, triggerRouter);
    }
    iter->destroy(iter);
    triggers->destroy(triggers);
    return triggerRouters;
}

static SymList * SymTriggerRouterService_getConfigurationTablesTriggerRoutersForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId) {
    SymList *triggerRouters = SymList_new(NULL);
    SymList *links = this->configurationService->getNodeGroupLinksFor(this->configurationService, sourceNodeGroupId, 0);
    SymIterator *iter = links->iterator(links);
    while (iter->hasNext(iter)) {
        SymNodeGroupLink *nodeGroupLink = (SymNodeGroupLink *) iter->next(iter);
        SymList *triggerRoutersSym = SymTriggerRouterService_buildTriggerRoutersForSymmetricTables(this, nodeGroupLink, NULL);
        triggerRouters->addAll(triggerRouters, triggerRoutersSym);
        triggerRoutersSym->destroy(triggerRoutersSym);
    }
    iter->destroy(iter);
    return triggerRouters;
}

static void SymTriggerRouterService_mergeInConfigurationTablesTriggerRoutersForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId,
        SymList *configuredInDatabase) {
    SymList *virtualConfigTriggers = SymTriggerRouterService_getConfigurationTablesTriggerRoutersForCurrentNode(this, sourceNodeGroupId);
    SymIterator *iter = virtualConfigTriggers->iterator(virtualConfigTriggers);
    while (iter->hasNext(iter)) {
        SymTriggerRouter *trigger = (SymTriggerRouter *) iter->next(iter);
        if (SymStringUtils_equalsIgnoreCase(trigger->router->nodeGroupLink->sourceNodeGroupId, sourceNodeGroupId) &&
                !SymTriggerRouterService_doesTriggerRouterExistInList(configuredInDatabase, trigger)) {
            configuredInDatabase->add(configuredInDatabase, trigger);
        }
    }

    virtualConfigTriggers->destroy(virtualConfigTriggers);
    iter->destroy(iter);
}

static SymList * SymTriggerRouterService_getAllTriggerRoutersForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId) {
    SymSqlTemplate *sqlTemplate = this->platform->getSqlTemplate(this->platform);
    SymStringBuilder *sql = SymStringBuilder_newWithString("select ");
    sql->append(sql, SYM_SQL_SELECT_TRIGGER_ROUTERS_COLUMN_LIST);
    sql->append(sql, SYM_SQL_SELECT_TRIGGER_ROUTERS);
    sql->append(sql, SYM_SQL_ACTIVE_TRIGGERS_FOR_SOURCE_NODE_GROUP);

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, sourceNodeGroupId);

    int error;
    SymList *triggerRouters = sqlTemplate->query(sqlTemplate, sql->str, args, NULL, &error, (void *) &SymTriggerRouterService_triggerRouterMapper);
    SymTriggerRouterService_enhanceTriggerRouters(this, triggerRouters);
    SymTriggerRouterService_mergeInConfigurationTablesTriggerRoutersForCurrentNode(this, sourceNodeGroupId, triggerRouters);

    sql->destroy(sql);
    args->destroy(args);
    return triggerRouters;
}

static SymTriggerRoutersCache * SymTriggerRouterService_getTriggerRoutersCacheForCurrentNode(SymTriggerRouterService * this, unsigned short refreshCache) {
    char *myNodeGroupId = this->parameterService->getNodeGroupId(this->parameterService);
    long triggerRouterCacheTimeoutInMs = this->parameterService->getLong(this->parameterService, SYM_PARAMETER_CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS, 600000);
    SymTriggerRoutersCache *cache = this->triggerRouterCacheByNodeGroupId == NULL ? NULL:
            this->triggerRouterCacheByNodeGroupId->get(this->triggerRouterCacheByNodeGroupId, myNodeGroupId);
    if (cache == NULL || refreshCache || (time(NULL) - this->triggerRouterPerNodeCacheTime) * 1000 > triggerRouterCacheTimeoutInMs) {
        this->triggerRouterPerNodeCacheTime = time(NULL);
        SymMap *newTriggerRouterCacheByNodeGroupId = SymMap_new(NULL, 10);
        SymList *triggerRouters = SymTriggerRouterService_getAllTriggerRoutersForCurrentNode(this, myNodeGroupId);
        SymMap *triggerRoutersByTriggerId = SymMap_new(NULL, triggerRouters->size);
        SymMap *routers = SymMap_new(NULL, triggerRouters->size);
        SymIterator *iter = triggerRouters->iterator(triggerRouters);
        while (iter->hasNext(iter)) {
            SymTriggerRouter *triggerRouter = (SymTriggerRouter *) iter->next(iter);
            if (triggerRouter->enabled) {
                char *triggerId = triggerRouter->trigger->triggerId;
                SymList *list = triggerRoutersByTriggerId->get(triggerRoutersByTriggerId, triggerId);
                if (list == NULL) {
                    list = SymList_new(NULL);
                    triggerRoutersByTriggerId->put(triggerRoutersByTriggerId, triggerId, list);
                }
                list->add(list, triggerRouter);
                routers->put(routers, triggerRouter->router->routerId, triggerRouter->router);
            }
        }
        iter->destroy(iter);
        triggerRouters->destroy(triggerRouters);

        cache = (SymTriggerRoutersCache *) calloc(1, sizeof(SymTriggerRoutersCache));
        cache->routersByRouterId = routers;
        cache->triggerRoutersByTriggerId = triggerRoutersByTriggerId;
        newTriggerRouterCacheByNodeGroupId->put(newTriggerRouterCacheByNodeGroupId, myNodeGroupId, cache);
        if (this->triggerRouterCacheByNodeGroupId) {
            this->triggerRouterCacheByNodeGroupId->destroy(this->triggerRouterCacheByNodeGroupId);
        }
        this->triggerRouterCacheByNodeGroupId = newTriggerRouterCacheByNodeGroupId;
        cache = this->triggerRouterCacheByNodeGroupId == NULL ? NULL:
                this->triggerRouterCacheByNodeGroupId->get(this->triggerRouterCacheByNodeGroupId, myNodeGroupId);
    }
    return cache;
}

SymMap * SymTriggerRouterService_getTriggerRoutersForCurrentNode(SymTriggerRouterService * this, unsigned short refreshCache) {
    SymTriggerRoutersCache *cache = SymTriggerRouterService_getTriggerRoutersCacheForCurrentNode(this, refreshCache);
    return cache->triggerRoutersByTriggerId;
}

void SymTriggerRouterService_destroy(SymTriggerRouterService * this) {
    this->historyMap->destroy(this->historyMap);
    if (this->routersCache)  {
        this->routersCache->destroy(this->routersCache);
    }
    if (this->triggersCache) {
        this->triggersCache->destroy(this->triggersCache);
    }
    if (this->triggerRouterCacheByNodeGroupId) {
        this->triggerRouterCacheByNodeGroupId->destroy(this->triggerRouterCacheByNodeGroupId);
    }
    free(this);
}

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService *this,
        SymConfigurationService *configurationService, SymSequenceService *sequenceService,
        SymParameterService *parameterService, SymDatabasePlatform *platform, SymDialect *symmetricDialect) {

    if (this == NULL) {
        this = (SymTriggerRouterService*) calloc(1, sizeof(SymTriggerRouterService));
    }

    this->historyMap = SymMap_new(NULL, 100);

    this->configurationService = configurationService;
    this->sequenceService = sequenceService;
    this->parameterService = parameterService;
    this->symmetricDialect = symmetricDialect;
    this->platform = platform;

    this->syncTriggers = (void *) &SymTriggerRouterService_syncTriggers;
    this->getTriggers = (void *) &SymTriggerRouterService_getTriggers;
    this->getTriggerById = (void *) &SymTriggerRouterService_getTriggerById;
    this->getTriggerHistory = (void *) &SymTriggerRouterService_getTriggerHistory;
    this->getActiveTriggerHistories = (void *) &SymTriggerRouterService_getActiveTriggerHistories;
    this->getActiveTriggerHistoriesByTrigger = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesByTrigger;
    this->getActiveTriggerHistoriesByTableName = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesByTableName;
    this->getRouters = (void *) &SymTriggerRouterService_getRouters;
    this->getRouterById = (void *) &SymTriggerRouterService_getRouterById;
    this->getTriggerRoutersForCurrentNode = (void *) &SymTriggerRouterService_getTriggerRoutersForCurrentNode;
    this->destroy = (void *) &SymTriggerRouterService_destroy;
    return this;
}
