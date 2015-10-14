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
    if (!Sym_isBlank(condition)) {
        trigger->syncOnInsertCondition = condition;
    }
    condition = row->getStringNew(row, "sync_on_update_condition");
    if (!Sym_isBlank(condition)) {
        trigger->syncOnUpdateCondition = condition;
    }
    condition = row->getStringNew(row, "sync_on_delete_condition");
    if (!Sym_isBlank(condition)) {
        trigger->syncOnDeleteCondition = condition;
    }

    char *text = row->getStringNew(row, "custom_on_insert_text");
    if (!Sym_isBlank(text)) {
        trigger->customOnInsertText = text;
    }
    text = row->getStringNew(row, "custom_on_update_text");
    if (!Sym_isBlank(text)) {
        trigger->customOnUpdateText = text;
    }
    text = row->getStringNew(row, "custom_on_delete_text");
    if (!Sym_isBlank(text)) {
        trigger->customOnDeleteText = text;
    }

    condition = row->getStringNew(row, "external_select");
    if (!Sym_isBlank(condition)) {
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

SymList * SymTriggerRouterService_getActiveTriggerHistories(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getRouters(SymTriggerRouterService *this, unsigned short replaceVariables) {
    // TODO
    return 0;
}

char * SymTriggerRouterService_getTriggerRouterSql(SymTriggerRouterService *this, char *sql) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getTriggerRouters(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_enhanceTriggerRouters(SymTriggerRouterService *this, SymList *triggerRouters) {
    // TODO
    return 0;
}

void SymTriggerRouterService_insert(SymTriggerRouterService *this, SymTriggerHistory *newHistRecord) {
    // TODO
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

void SymTriggerRouterService_inactivateTriggers(SymTriggerRouterService *this, SymList *triggersThatShouldBeActive, SymList *activeTriggerHistories) {
    // TODO
}

void SymTriggerRouterService_dropTriggers(SymTriggerRouterService *this, SymTriggerHistory *history) {
    // TODO
}

void SymTriggerRouterService_updateOrCreateDatabaseTriggers(SymTriggerRouterService *this, SymTrigger *trigger, SymTable *table, SymList *activeTriggerHistories, unsigned short force) {
    // TODO
}

SymTriggerHistory * SymTriggerRouterService_rebuildTriggerIfNecessary(SymTriggerRouterService *this, unsigned short forceRebuild, SymTrigger *trigger, SymDataEventType *dmlType, char *reason, SymTriggerHistory *oldhist, SymTriggerHistory *hist, unsigned short triggerIsActive, SymTable *table, SymList *activeTriggerHistories) {
    // TODO
    return 0;
}

char * SymTriggerRouterService_replaceCharsToShortenName(SymTriggerRouterService *this, char *triggerName) {
    // TODO
    return 0;
}

char * SymTriggerRouterService_getTriggerName(SymTriggerRouterService *this, SymDataEventType *dml, int maxTriggerNameLength, SymTrigger *trigger, SymTable *table, SymList *activeTriggerHistories) {
    // TODO
    return 0;
}

SymTriggerHistory * SymTriggerHistoryMapper_mapRow(SymTriggerRouterService *this, SymRow *rs) {
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
             SymTriggerRouterService_updateOrCreateDatabaseTriggers(this, trigger, table, activeTriggerHistories, force);
         }
    }
}

void SymTriggerRouterService_destroy(SymTriggerRouterService * this) {
    free(this);
}

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService *this,
        SymConfigurationService *configurationService, SymSequenceService *sequenceService,
        SymParameterService *parameterService, SymDatabasePlatform *platform) {

    if (this == NULL) {
        this = (SymTriggerRouterService*) calloc(1, sizeof(SymTriggerRouterService));
    }

    this->configurationService = configurationService;
    this->sequenceService = sequenceService;
    this->parameterService = parameterService;
    this->platform = platform;

    this->syncTriggers = (void *) &SymTriggerRouterService_syncTriggers;

    this->destroy = (void *) &SymTriggerRouterService_destroy;
    return this;
}
