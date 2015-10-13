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
#include <service/TriggerRouterService.h>
#include "common/ParameterConstants.h"
#include "common/Log.h"
#include "model/TriggerRouter.h"
#include <time.h>

#define SYM_SQL_TRIGGER_ROUTERS_COLUMN_LIST "select tr.trigger_id, tr.router_id, tr.create_time, tr.last_update_time, tr.last_update_by, tr.initial_load_order, tr.initial_load_select, tr.initial_load_delete_stmt, tr.initial_load_batch_count, tr.ping_back_enabled, tr.enabled "
#define SYM_SQL_TRIGGER_ROUTERS     "from sym_trigger_router tr \
inner join sym_trigger t on tr.trigger_id=t.trigger_id \
inner join sym_router r on tr.router_id=r.router_id      "

static SymTriggerRouter * SymTriggerRouterService_triggerRouterMapper(SymRow *row) {
    SymTriggerRouter *triggerRouter = SymTriggerRouter_new(NULL);
    triggerRouter->enabled = row->getBoolean(row, "enabled");
    triggerRouter->initialLoadOrder = row->getInt(row, "initial_load_order");
    triggerRouter->initialLoadSelect = row->getStringNew(row, "initial_load_select");
    triggerRouter->initialLoadDeleteStmt = row->getStringNew(row, "initial_load_delete_stmt");
    triggerRouter->initialLoadBatchCount = row->getInt(row, "initial_load_batch_count");

    // TODO load up the Trigger and the Router.

    triggerRouter->createTime = row->getDate(row, "create_time");
    triggerRouter->lastUpdateTime = row->getDate(row, "last_update_time");
    triggerRouter->lastUpdateBy = row->getStringNew(row, "last_update_by");
    triggerRouter->pingBackEnabled = row->getBoolean(row, "pingBackEnabled");

    return triggerRouter;
}

SymList * SymTriggerSelector_select(SymTriggerSelector *this) {
	SymList *filtered = SymList_new(NULL);

	if (this->triggers == NULL) {
		return filtered;
	}

	SymIterator *iter = this->triggers->iterator(this->triggers);
    while (iter->hasNext(iter)) {
    	SymTriggerRouter *trigger = (SymTriggerRouter *) iter->next(iter);

    	// TODO check for duplicate trigger ID's. -- if (!filtered.contains(trigger.getTrigger()))
    	filtered->add(filtered, trigger);
    }
	return filtered;
}

void SymTriggerSelector_destroy(SymTriggerSelector * this) {
	this->triggers->destroy(this->triggers);
	free(this);
}

SymTriggerSelector * SymTriggerSelector_new(SymTriggerSelector *this, SymList *triggers) {
    if (this == NULL) {
        this = (SymTriggerSelector*) calloc(1, sizeof(SymTriggerSelector));
    }
    this->triggers = triggers;
    this->select = (void *) &SymTriggerSelector_select;

    this->destroy = (void *) &SymTriggerSelector_destroy;
    return this;
}

void SymTriggerRoutersCache_destroy(SymTriggerSelector * this) {
	this->triggers->destroy(this->triggers);
	free(this);
}

SymTriggerRoutersCache * SymTriggerRoutersCache_new(SymTriggerRoutersCache *this) {
    if (this == NULL) {
        this = (SymTriggerRoutersCache*) calloc(1, sizeof(SymTriggerRoutersCache));
    }
    this->destroy = (void *) &SymTriggerRoutersCache_destroy;
    return this;
}

unsigned short SymTriggerRouterService_refreshFromDatabase(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getTriggers(SymTriggerRouterService *this, unsigned short replaceTokens) {
    // TODO
    return NULL;
}

unsigned short SymTriggerRouterService_isTriggerBeingUsed(SymTriggerRouterService *this, char *triggerId) {
    // TODO
    return 0;
}

unsigned short SymTriggerRouterService_doesTriggerExist(SymTriggerRouterService *this, char *triggerId) {
    // TODO
    return 0;
}

unsigned short SymTriggerRouterService_doesTriggerExistForTable(SymTriggerRouterService *this, char *tableName) {
    // TODO
    return 0;
}

void SymTriggerRouterService_deleteTrigger(SymTriggerRouterService *this, SymTrigger *trigger) {
    // TODO
}

void SymTriggerRouterService_dropTriggers(SymTriggerRouterService *this) {
    // TODO
}

void SymTriggerRouterService_dropTriggersForTables(SymTriggerRouterService *this, SymList *tables) {
    // TODO
}

void SymTriggerRouterService_deleteTriggerHistory(SymTriggerRouterService *this, SymTriggerHistory *history) {
    // TODO
}

void SymTriggerRouterService_createTriggersOnChannelForTables(SymTriggerRouterService *this, char *channelId, char *catalogName, char *schemaName, SymList *tables, char *lastUpdateBy) {
    // TODO
}

SymList * SymTriggerRouterService_createTriggersOnChannelForTablesWithReturn(SymTriggerRouterService *this, char *channelId, char *catalogName, char *schemaName, SymList *tables, char *lastUpdateBy) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_findMatchingTriggers(SymTriggerRouterService *this, SymList *triggers, char *catalog, char *schema, char *table) {
    // TODO
    return 0;
}

void SymTriggerRouterService_inactivateTriggerHistory(SymTriggerRouterService *this, SymTriggerHistory *history) {
    // TODO
}

SymMap * SymTriggerRouterService_getHistoryRecords(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

unsigned short SymTriggerRouterService_isTriggerNameInUse(SymTriggerRouterService *this, SymList *activeTriggerHistories, char *triggerId, char *triggerName) {
    // TODO
    return 0;
}

SymTriggerHistory * SymTriggerRouterService_findTriggerHistory(SymTriggerRouterService *this, char *catalogName, char *schemaName, char *tableName) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_findTriggerHistories(SymTriggerRouterService *this, char *catalogName, char *schemaName, char *tableName) {
    // TODO
    return 0;
}

SymTriggerHistory * SymTriggerRouterService_getTriggerHistory(SymTriggerRouterService *this, int histId) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getActiveTriggerHistoriesForTrigger(SymTriggerRouterService *this, SymTrigger *trigger) {
    // TODO
    return 0;
}

SymTriggerHistory * SymTriggerRouterService_getNewestTriggerHistoryForTrigger(SymTriggerRouterService *this, char *triggerId, char *catalogName, char *schemaName, char *tableName) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getActiveTriggerHistoriesFromCache(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getActiveTriggerHistories(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getActiveTriggerHistoriesForTable(SymTriggerRouterService *this, char *tableName) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_buildTriggersForSymmetricTables(SymTriggerRouterService *this, char *version, char *tablesToExclude) {
    // TODO
    return 0;
}

SymTrigger * SymTriggerRouterService_buildTriggerForSymmetricTable(SymTriggerRouterService *this, char *tableName) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_buildTriggerRoutersForSymmetricTables(SymTriggerRouterService *this, char *version, SymNodeGroupLink *nodeGroupLink, char *tablesToExclude) {
    // TODO
    return 0;
}

char * SymTriggerRouterService_buildSymmetricTableRouterId(SymTriggerRouterService *this, char *triggerId, char *sourceNodeGroupId, char *targetNodeGroupId) {
    // TODO
    return 0;
}

SymTriggerRouter * SymTriggerRouterService_buildTriggerRoutersForSymmetricTablesWithNodeGroupLink(SymTriggerRouterService *this, char *version, SymTrigger *trigger, SymNodeGroupLink *nodeGroupLink) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getTriggerRouterForTableForCurrentNode(SymTriggerRouterService *this, SymNodeGroupLink *link, char *catalogName, char *schemaName, char *tableName, unsigned short refreshCache) {
    // TODO
    return 0;
}

unsigned short SymTriggerRouterService_isMatch(SymTriggerRouterService *this, SymNodeGroupLink *link, SymTriggerRouter *router) {
    // TODO
    return 0;
}

unsigned short SymTriggerRouterService_isMatchTableName(SymTriggerRouterService *this, char *catalogName, char *schemaName, char *tableName, SymTrigger *trigger) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getConfigurationTablesTriggerRoutersForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId) {
    // TODO
    return 0;
}

void SymTriggerRouterService_mergeInConfigurationTablesTriggerRoutersForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId, SymList *configuredInDatabase) {
    // TODO
}

unsigned short SymTriggerRouterService_doesTriggerRouterExistInList(SymTriggerRouterService *this, SymList *triggerRouters, SymTriggerRouter *triggerRouter) {
    // TODO
    return 0;
}

SymTriggerRouter * SymTriggerRouterService_getTriggerRouterForCurrentNode(SymTriggerRouterService *this, char *triggerId, char *routerId, unsigned short refreshCache) {
    // TODO
    return 0;
}

SymMap* SymTriggerRouterService_getTriggerRoutersForCurrentNode(SymTriggerRouterService *this, unsigned short refreshCache) {
    return this->getTriggerRoutersCacheForCurrentNode(this, refreshCache)->triggerRoutersByTriggerId;
}

SymList* SymTriggerRouterService_getTriggersForCurrentNode(SymTriggerRouterService *this, unsigned short refreshCache) {
    SymMap *triggerRouters = SymTriggerRouterService_getTriggerRoutersForCurrentNode(this, refreshCache);
    SymList *triggers = SymList_new(NULL);



    //  SymTriggerSelector *triggerSelector = SymTriggerSelector_new(NULL, NULL /** todo */);
    //  return triggerSelector->select(triggerSelector);
//  SymMap *triggerRouters = this->getTriggerRoutersCacheForCurrentNode(this, refreshCache);
    // TODO >>>>>>>>

    return NULL;
}

SymTriggerRoutersCache* SymTriggerRouterService_getTriggerRoutersCacheForCurrentNode(SymTriggerRouterService *this, unsigned short refreshCache) {
    char* myNodeGroupId = this->parameterService->getNodeGroupId(this->parameterService);
    long triggerRouterCacheTimeoutInMs = this->parameterService->getLong(this->parameterService, CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS, 1000);
    SymTriggerRoutersCache *cache = this->triggerRouterCacheByNodeGroupId == NULL ? NULL
            : this->triggerRouterCacheByNodeGroupId->get(this->triggerRouterCacheByNodeGroupId, myNodeGroupId);
    time_t currentTimeMillis = time(NULL)*1000;

    if (cache == NULL
            || refreshCache
            || currentTimeMillis - this->triggerRouterPerNodeCacheTime > triggerRouterCacheTimeoutInMs) {
        this->triggerRouterPerNodeCacheTime = currentTimeMillis;
        SymMap *newTriggerRouterCacheByNodeGroupId = SymMap_new(NULL, 8);
        SymList *triggerRouters = this->getAllTriggerRoutersForCurrentNode(this, myNodeGroupId);
        // TODO <<<<<<<<<
    }

    return NULL;
}

SymRouter * SymTriggerRouterService_getActiveRouterByIdForCurrentNode(SymTriggerRouterService *this, char *routerId, unsigned short refreshCache) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getRoutersByGroupLink(SymTriggerRouterService *this, SymNodeGroupLink *link) {
    // TODO
    return 0;
}

SymTrigger * SymTriggerRouterService_getTriggerForCurrentNodeById(SymTriggerRouterService *this, char *triggerId) {
    // TODO
    return 0;
}

SymTrigger * SymTriggerRouterService_getTriggerById(SymTriggerRouterService *this, char *triggerId, unsigned short refreshCache) {
    // TODO
    return 0;
}

SymRouter * SymTriggerRouterService_getRouterById(SymTriggerRouterService *this, char *routerId, unsigned short refreshCache) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getRouters(SymTriggerRouterService *this, unsigned short replaceVariables) {
    // TODO
    return 0;
}

char * SymTriggerRouterService_getTriggerRouterSql(SymTriggerRouterService *this, char *sql) {
    char *sqlSuffix = "";
    if (strcmp(sql, "activeTriggersForSourceNodeGroupSql")) {
        sqlSuffix = " where r.source_node_group_id = ? ";
    }

    SymStringBuilder *buff = SymStringBuilder_new(NULL);
    buff->append(buff, SYM_SQL_TRIGGER_ROUTERS_COLUMN_LIST);
    buff->append(buff, SYM_SQL_TRIGGER_ROUTERS);
    buff->append(buff, sqlSuffix);

    return buff->destroyAndReturn(buff);
}

SymList * SymTriggerRouterService_getTriggerRouters(SymTriggerRouterService *this, unsigned short refreshCache) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getAllTriggerRoutersForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId) {

    char *triggerRouterSql = this->getTriggerRouterSql(this, "activeTriggersForSourceNodeGroupSql");

    SymStringArray *args = SymStringArray_new(NULL);
    args->add(args, sourceNodeGroupId);
    int error;
    SymList* triggers = this->sqlTemplate->query(this->sqlTemplate, triggerRouterSql, args, NULL, &error, (void *) SymTriggerRouterService_triggerRouterMapper);
    args->destroy(args);

    SymList *triggerRouters =
            this->enhanceTriggerRouters(this, triggers);



    return 0;
}

SymList * SymTriggerRouterService_getAllTriggerRoutersForReloadForCurrentNode(SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId) {
    // TODO
    return 0;
}

SymTriggerRouter * SymTriggerRouterService_findTriggerRouterById(SymTriggerRouterService *this, char *triggerId, char *routerId) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_enhanceTriggerRouters(SymTriggerRouterService *this, SymList *triggerRouters) {
    // TODO
    return 0;
}

SymMap * SymTriggerRouterService_getTriggerRoutersByChannel(SymTriggerRouterService *this, char *nodeGroupId, unsigned short refreshCache) {
    // TODO
    return 0;
}

void SymTriggerRouterService_insert(SymTriggerRouterService *this, SymTriggerHistory *newHistRecord) {
    // TODO
}

void SymTriggerRouterService_deleteTriggerRouterWithId(SymTriggerRouterService *this, char *triggerId, char *routerId) {
    // TODO
}

void SymTriggerRouterService_deleteTriggerRouter(SymTriggerRouterService *this, SymTriggerRouter *triggerRouter) {
    // TODO
}

void SymTriggerRouterService_saveTriggerRouter(SymTriggerRouterService *this, SymTriggerRouter *triggerRouter, unsigned shortupdateTriggerRouterTableOnly) {
    // TODO
}

void SymTriggerRouterService_resetTriggerRouterCacheByNodeGroupId(SymTriggerRouterService *this) {
    // TODO
}

void SymTriggerRouterService_saveRouter(SymTriggerRouterService *this, SymRouter *router) {
    // TODO
}

unsigned short SymTriggerRouterService_isRouterBeingUsed(SymTriggerRouterService *this, char *routerId) {
    // TODO
    return 0;
}

void SymTriggerRouterService_deleteRouter(SymTriggerRouterService *this, SymRouter *router) {
    // TODO
}

void SymTriggerRouterService_saveTrigger(SymTriggerRouterService *this, SymTrigger *trigger) {
    // TODO
}

void SymTriggerRouterService_clearCache(SymTriggerRouterService *this) {
    this->triggerRouterPerNodeCacheTime = 0;
    this->triggerRouterPerChannelCacheTime = 0;
    this->triggerRoutersCacheTime = 0;
    this->routersCacheTime = 0;
    this->triggersCacheTime = 0;
}

SymList * SymTriggerRouterService_getTriggerIdsFrom(SymTriggerRouterService *this, SymList *triggersThatShouldBeActive) {
    // TODO
    return 0;
}

SymTrigger * SymTriggerRouterService_getTriggerFromList(SymTriggerRouterService *this, char *triggerId, SymList *triggersThatShouldBeActive) {
    // TODO
    return 0;
}

void SymTriggerRouterService_inactivateTriggers(SymTriggerRouterService *this, SymList *triggersThatShouldBeActive, SymStringBuilder *sqlBuffer, SymList *activeTriggerHistories) {
    // TODO
}

unsigned short SymTriggerRouterService_isEqual(SymTriggerRouterService *this, char *one, char *two, unsigned short ignoreCase) {
    // TODO
    return 0;
}

void SymTriggerRouterService_dropTriggersForTriggerHistory(SymTriggerRouterService *this, SymTriggerHistory *history, SymStringBuilder *sqlBuffer) {
    // TODO
}

SymList * SymTriggerRouterService_toList(SymTriggerRouterService *this, SymList *source) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getTablesForTrigger(SymTriggerRouterService *this, SymTrigger *trigger, SymList *triggers, unsigned shortuseTableCache) {
    // TODO
    return 0;
}

unsigned short SymTriggerRouterService_containsExactMatchForSourceTableName(SymTriggerRouterService *this, SymTable *table, SymList *triggers, unsigned short ignoreCase) {
    // TODO
    return 0;
}

void SymTriggerRouterService_updateOrCreateDatabaseTriggers(SymTriggerRouterService *this, SymList *triggers, SymStringBuilder *sqlBuffer, unsigned short force, unsigned short verifyInDatabase, SymList *activeTriggerHistories, unsigned shortuseTableCache) {
    // TODO
}

void SymTriggerRouterService_updateOrCreateDatabaseTrigger(SymTriggerRouterService *this, SymTrigger *trigger, SymList *triggers, SymStringBuilder *sqlBuffer, unsigned short force, unsigned short verifyInDatabase, SymList *activeTriggerHistories, unsigned shortuseTableCache) {
    // TODO
}

void SymTriggerRouterService_syncTrigger(SymTriggerRouterService *this, SymTrigger *trigger, unsigned short force, unsigned short verifyInDatabase) {
    // TODO
}

void SymTriggerRouterService_updateOrCreateDatabaseTriggersForTable(SymTriggerRouterService *this, SymTrigger *trigger, SymTable *table, SymStringBuilder *sqlBuffer, unsigned short force, unsigned short verifyInDatabase, SymList *activeTriggerHistories) {
    // TODO
}

SymTriggerHistory * SymTriggerRouterService_rebuildTriggerIfNecessary(SymTriggerRouterService *this, SymStringBuilder *sqlBuffer, unsigned short forceRebuild, SymTrigger *trigger, SymDataEventType *dmlType, char *reason, SymTriggerHistory *oldhist, SymTriggerHistory *hist, unsigned short triggerIsActive, SymTable *table, SymList *activeTriggerHistories) {
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

//SymTriggerHistory * SymTriggerHistoryMapper_mapRow(SymTriggerRouterService *this, SymRow *rs) {
//    // TODO
//}

SymNodeGroupLink * SymRouterMapper_getNodeGroupLink(SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId) {
    // TODO
    return 0;
}

//SymRouter * SymRouterMapper_mapRow(SymTriggerRouterService *this, SymRow *rs) {
//    // TODO
//}
//
//SymTrigger * SymTriggerMapper_mapRow(SymTriggerRouterService *this, SymRow *rs) {
//    // TODO
//}
//
//SymTriggerRouter * SymTriggerRouterMapper_mapRow(SymTriggerRouterService *this, SymRow *rs) {
//    // TODO
//}

void SymTriggerRouterService_addExtraConfigTable(SymTriggerRouterService *this, char *table) {
    // TODO
}

SymMap * SymTriggerRouterService_getFailedTriggers(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymTriggerHistory * SymTriggerRouterService_findTriggerHistoryForGenericSync(SymTriggerRouterService *this) {
    // TODO
    return 0;
}

SymMap * SymTriggerRouterService_fillTriggerRoutersByHistIdAndSortHist(SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId, SymList *triggerHistories) {
    // TODO
    return 0;
}

SymList * SymTriggerRouterService_getSortedTablesFor(SymTriggerRouterService *this, SymList *histories) {
    // TODO
    return 0;
}



int SymTriggerRouterService_syncTriggers (SymTriggerRouterService *this, SymStringBuilder *sqlBuffer, unsigned short force) {
	// This gets called by SymEngine at startup.
	SymLog_info("SymTriggerRouterService_syncTriggers_withTable");

	unsigned short autoSyncTriggers = this->parameterService->is(this->parameterService, AUTO_SYNC_TRIGGERS, 1);

	if (autoSyncTriggers) {
		 SymLog_info("Synchronizing triggers");
		 this->platform->resetCachedTableModel(this->platform);
		 this->clearCache(this);
		 this->configurationService->clearCache(this->configurationService);

		 SymList* triggersForCurrentNode = this->getTriggersForCurrentNode(this, 0);

		 // TODO >>>>>>>>
	}

    return 0;
}

int SymTriggerRouterService_syncTriggersWithTable(SymTriggerRouterService *this, SymTable *table, unsigned short force) {
	// TODO
	unsigned short ignoreCase = this->parameterService->is(this->parameterService, AUTO_SYNC_TRIGGERS_AT_STARTUP, 0);

	SymLog_info("SymTriggerRouterService_syncTriggers_withTable");
    return 0;
}

void SymTriggerRouterService_destroy(SymTriggerRouterService * this) {
    free(this);
}

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService * this, SymParameterService *parameterService, SymDatabasePlatform *platform, SymConfigurationService *configurationService) {
    if (this == NULL) {
        this = (SymTriggerRouterService*) calloc(1, sizeof(SymTriggerRouterService));
    }
    this->parameterService = parameterService;
    this->platform = platform;
    this->configurationService = configurationService;

    this->refreshFromDatabase = (void *) &SymTriggerRouterService_refreshFromDatabase;
    this->getTriggers = (void *) &SymTriggerRouterService_getTriggers;
    this->getTriggers = (void *) &SymTriggerRouterService_getTriggers;
    this->isTriggerBeingUsed = (void *) &SymTriggerRouterService_isTriggerBeingUsed;
    this->doesTriggerExist = (void *) &SymTriggerRouterService_doesTriggerExist;
    this->doesTriggerExistForTable = (void *) &SymTriggerRouterService_doesTriggerExistForTable;
    this->deleteTrigger = (void *) &SymTriggerRouterService_deleteTrigger;
    this->dropTriggers = (void *) &SymTriggerRouterService_dropTriggers;
    this->dropTriggersForTables = (void *) &SymTriggerRouterService_dropTriggersForTables;
    this->deleteTriggerHistory = (void *) &SymTriggerRouterService_deleteTriggerHistory;
    this->createTriggersOnChannelForTables = (void *) &SymTriggerRouterService_createTriggersOnChannelForTables;
    this->createTriggersOnChannelForTablesWithReturn = (void *) &SymTriggerRouterService_createTriggersOnChannelForTablesWithReturn;
    this->findMatchingTriggers = (void *) &SymTriggerRouterService_findMatchingTriggers;
    this->inactivateTriggerHistory = (void *) &SymTriggerRouterService_inactivateTriggerHistory;
    this->getHistoryRecords = (void *) &SymTriggerRouterService_getHistoryRecords;
    this->isTriggerNameInUse = (void *) &SymTriggerRouterService_isTriggerNameInUse;
    this->findTriggerHistory = (void *) &SymTriggerRouterService_findTriggerHistory;
    this->findTriggerHistories = (void *) &SymTriggerRouterService_findTriggerHistories;
    this->getTriggerHistory = (void *) &SymTriggerRouterService_getTriggerHistory;
    this->getActiveTriggerHistoriesForTrigger = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesForTrigger;
    this->getNewestTriggerHistoryForTrigger = (void *) &SymTriggerRouterService_getNewestTriggerHistoryForTrigger;
    this->getActiveTriggerHistoriesFromCache = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesFromCache;
    this->getActiveTriggerHistories = (void *) &SymTriggerRouterService_getActiveTriggerHistories;
    this->getActiveTriggerHistoriesForTable = (void *) &SymTriggerRouterService_getActiveTriggerHistoriesForTable;
    this->buildTriggersForSymmetricTables = (void *) &SymTriggerRouterService_buildTriggersForSymmetricTables;
    this->buildTriggerForSymmetricTable = (void *) &SymTriggerRouterService_buildTriggerForSymmetricTable;
    this->buildTriggerRoutersForSymmetricTables = (void *) &SymTriggerRouterService_buildTriggerRoutersForSymmetricTables;
    this->buildSymmetricTableRouterId = (void *) &SymTriggerRouterService_buildSymmetricTableRouterId;
    this->buildTriggerRoutersForSymmetricTablesWithNodeGroupLink = (void *) &SymTriggerRouterService_buildTriggerRoutersForSymmetricTablesWithNodeGroupLink;
    this->getTriggerRouterForTableForCurrentNode = (void *) &SymTriggerRouterService_getTriggerRouterForTableForCurrentNode;
    this->isMatch = (void *) &SymTriggerRouterService_isMatch;
    this->isMatchTableName = (void *) &SymTriggerRouterService_isMatchTableName;
    this->getConfigurationTablesTriggerRoutersForCurrentNode = (void *) &SymTriggerRouterService_getConfigurationTablesTriggerRoutersForCurrentNode;
    this->mergeInConfigurationTablesTriggerRoutersForCurrentNode = (void *) &SymTriggerRouterService_mergeInConfigurationTablesTriggerRoutersForCurrentNode;
    this->doesTriggerRouterExistInList = (void *) &SymTriggerRouterService_doesTriggerRouterExistInList;
    this->getTriggerRouterForCurrentNode = (void *) &SymTriggerRouterService_getTriggerRouterForCurrentNode;
    this->getTriggerRoutersForCurrentNode = (void *) &SymTriggerRouterService_getTriggerRoutersForCurrentNode;
    this->getTriggersForCurrentNode = (void *) &SymTriggerRouterService_getTriggersForCurrentNode;
    this->getTriggerRoutersCacheForCurrentNode = (void *) &SymTriggerRouterService_getTriggerRoutersCacheForCurrentNode;
    this->getActiveRouterByIdForCurrentNode = (void *) &SymTriggerRouterService_getActiveRouterByIdForCurrentNode;
    this->getRoutersByGroupLink = (void *) &SymTriggerRouterService_getRoutersByGroupLink;
    this->getTriggerForCurrentNodeById = (void *) &SymTriggerRouterService_getTriggerForCurrentNodeById;
    this->getTriggerById = (void *) &SymTriggerRouterService_getTriggerById;
    this->getRouterById = (void *) &SymTriggerRouterService_getRouterById;
    this->getRouters = (void *) &SymTriggerRouterService_getRouters;
    this->getTriggerRouterSql = (void *) &SymTriggerRouterService_getTriggerRouterSql;
    this->getTriggerRouters = (void *) &SymTriggerRouterService_getTriggerRouters;
    this->getAllTriggerRoutersForCurrentNode = (void *) &SymTriggerRouterService_getAllTriggerRoutersForCurrentNode;
    this->getAllTriggerRoutersForReloadForCurrentNode = (void *) &SymTriggerRouterService_getAllTriggerRoutersForReloadForCurrentNode;
    this->findTriggerRouterById = (void *) &SymTriggerRouterService_findTriggerRouterById;
    this->enhanceTriggerRouters = (void *) &SymTriggerRouterService_enhanceTriggerRouters;
    this->getTriggerRoutersByChannel = (void *) &SymTriggerRouterService_getTriggerRoutersByChannel;
    this->getTriggerRoutersByChannel = (void *) &SymTriggerRouterService_getTriggerRoutersByChannel;
    this->insert = (void *) &SymTriggerRouterService_insert;
    this->deleteTriggerRouterWithId = (void *) &SymTriggerRouterService_deleteTriggerRouterWithId;
    this->deleteTriggerRouter = (void *) &SymTriggerRouterService_deleteTriggerRouter;
    this->saveTriggerRouter = (void *) &SymTriggerRouterService_saveTriggerRouter;
    this->resetTriggerRouterCacheByNodeGroupId = (void *) &SymTriggerRouterService_resetTriggerRouterCacheByNodeGroupId;
    this->saveRouter = (void *) &SymTriggerRouterService_saveRouter;
    this->isRouterBeingUsed = (void *) &SymTriggerRouterService_isRouterBeingUsed;
    this->deleteRouter = (void *) &SymTriggerRouterService_deleteRouter;
    this->saveTrigger = (void *) &SymTriggerRouterService_saveTrigger;
    this->clearCache = (void *) &SymTriggerRouterService_clearCache;
    this->getTriggerIdsFrom = (void *) &SymTriggerRouterService_getTriggerIdsFrom;
    this->getTriggerFromList = (void *) &SymTriggerRouterService_getTriggerFromList;
    this->inactivateTriggers = (void *) &SymTriggerRouterService_inactivateTriggers;
    this->isEqual = (void *) &SymTriggerRouterService_isEqual;
    this->dropTriggersForTriggerHistory = (void *) &SymTriggerRouterService_dropTriggersForTriggerHistory;
    this->toList = (void *) &SymTriggerRouterService_toList;
    this->getTablesForTrigger = (void *) &SymTriggerRouterService_getTablesForTrigger;
    this->containsExactMatchForSourceTableName = (void *) &SymTriggerRouterService_containsExactMatchForSourceTableName;
    this->updateOrCreateDatabaseTriggers = (void *) &SymTriggerRouterService_updateOrCreateDatabaseTriggers;
    this->updateOrCreateDatabaseTrigger = (void *) &SymTriggerRouterService_updateOrCreateDatabaseTrigger;
    this->syncTrigger = (void *) &SymTriggerRouterService_syncTrigger;
    this->updateOrCreateDatabaseTriggersForTable = (void *) &SymTriggerRouterService_updateOrCreateDatabaseTriggersForTable;
    this->rebuildTriggerIfNecessary = (void *) &SymTriggerRouterService_rebuildTriggerIfNecessary;
    this->replaceCharsToShortenName = (void *) &SymTriggerRouterService_replaceCharsToShortenName;
    this->getTriggerName = (void *) &SymTriggerRouterService_getTriggerName;
//    this->mapRow = (void *) &SymTriggerHistoryMapper_mapRow;
    this->getNodeGroupLink = (void *) &SymRouterMapper_getNodeGroupLink;
//    this->mapRow = (void *) &SymRouterMapper_mapRow;
//    this->mapRow = (void *) &SymTriggerMapper_mapRow;
//    this->mapRow = (void *) &SymTriggerRouterMapper_mapRow;
    this->addExtraConfigTable = (void *) &SymTriggerRouterService_addExtraConfigTable;
    this->getFailedTriggers = (void *) &SymTriggerRouterService_getFailedTriggers;
    this->findTriggerHistoryForGenericSync = (void *) &SymTriggerRouterService_findTriggerHistoryForGenericSync;
    this->fillTriggerRoutersByHistIdAndSortHist = (void *) &SymTriggerRouterService_fillTriggerRoutersByHistIdAndSortHist;
    this->getSortedTablesFor = (void *) &SymTriggerRouterService_getSortedTablesFor;


    this->syncTriggers = (void *) &SymTriggerRouterService_syncTriggers;
    this->syncTriggersWithTable = (void *) &SymTriggerRouterService_syncTriggersWithTable;
    this->destroy = (void *) &SymTriggerRouterService_destroy;
    return this;
}
