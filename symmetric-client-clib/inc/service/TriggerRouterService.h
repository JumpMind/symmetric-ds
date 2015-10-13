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
#ifndef SYM_TRIGGER_ROUTER_SERVICE_H
#define SYM_TRIGGER_ROUTER_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include "db/model/Table.h"
#include "service/ParameterService.h"
#include "service/ConfigurationService.h"
#include "db/platform/DatabasePlatform.h"
#include "util/Map.h"
#include "util/List.h"
#include "model/Trigger.h"
#include "model/TriggerHistory.h"
#include "model/NodeGroupLink.h"
#include "model/TriggerRouter.h"
#include "model/Router.h"
#include "io/data/DataEventType.h"
#include "db/sql/SqlTemplate.h"


typedef struct SymTriggerSelector {
    SymList* triggers;
    struct SymList * (*select)(struct SymTriggerSelector *this);
    void (*destroy)(struct SymTriggerSelector * this);
} SymTriggerSelector;

SymTriggerSelector * SymTriggerSelector_new(SymTriggerSelector *this, SymList *triggers);


typedef struct SymTriggerRoutersCache {
	SymMap* triggerRoutersByTriggerId;
	SymMap* routersByRouterId;
	void (*destroy)(struct SymTriggerSelector * this);
} SymTriggerRoutersCache;

SymTriggerRoutersCache * SymTriggerRoutersCache_new(SymTriggerRoutersCache *this);


typedef struct SymTriggerRouterService {
	long routersCacheTime;
	SymMap* routersCache;

    SymList* triggerRoutersCache;
    long triggerRouterPerNodeCacheTime;

    SymMap* triggersCache;
    long triggersCacheTime;
    long triggerRoutersCacheTime;

    SymMap* triggerRouterCacheByNodeGroupId;

    long triggerRouterPerChannelCacheTime;

	SymParameterService *parameterService;
    SymConfigurationService *configurationService;
	SymDatabasePlatform *platform;
	SymSqlTemplate *sqlTemplate;

	unsigned short (*refreshFromDatabase)(struct SymTriggerRouterService *this);
	SymList* (*getTriggers)(struct SymTriggerRouterService *this, unsigned short replaceTokens);
	unsigned short (*isTriggerBeingUsed)(struct SymTriggerRouterService *this, char *triggerId);
	unsigned short (*doesTriggerExist)(struct SymTriggerRouterService *this, char *triggerId);
	unsigned short (*doesTriggerExistForTable)(struct SymTriggerRouterService *this, char *tableName);
	void (*deleteTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger);
	void (*dropTriggers)(struct SymTriggerRouterService *this);
	void (*dropTriggersForTables)(struct SymTriggerRouterService *this, SymList *tables);
	void (*deleteTriggerHistory)(struct SymTriggerRouterService *this, SymTriggerHistory *history);
	void (*createTriggersOnChannelForTables)(struct SymTriggerRouterService *this, char *channelId, char *catalogName,
			char *schemaName, SymList *tables, char *lastUpdateBy);
	SymList* (*createTriggersOnChannelForTablesWithReturn)(struct SymTriggerRouterService *this,
			char *channelId, char *catalogName, char *schemaName, SymList *tables, char *lastUpdateBy);
	SymList* (*findMatchingTriggers)(struct SymTriggerRouterService *this, SymList *triggers,
			char *catalog, char *schema, char *table);
	void (*inactivateTriggerHistory)(struct SymTriggerRouterService *this, SymTriggerHistory *history);
	SymMap* (*getHistoryRecords)(struct SymTriggerRouterService *this);
	unsigned short (*isTriggerNameInUse)(struct SymTriggerRouterService *this, SymList *activeTriggerHistories,
			char *triggerId, char *triggerName);
	SymTriggerHistory* (*findTriggerHistory)(struct SymTriggerRouterService *this,
			char *catalogName, char *schemaName, char *tableName);
	SymList* (*findTriggerHistories)(struct SymTriggerRouterService *this,
			char *catalogName, char *schemaName, char *tableName);
	SymTriggerHistory* (*getTriggerHistory)(struct SymTriggerRouterService *this, int histId);
	SymList* (*getActiveTriggerHistoriesForTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger);
	SymTriggerHistory* (*getNewestTriggerHistoryForTrigger)(struct SymTriggerRouterService *this,
			char *triggerId, char *catalogName, char *schemaName, char *tableName);
	SymList* (*getActiveTriggerHistoriesFromCache)(struct SymTriggerRouterService *this);
	SymList* (*getActiveTriggerHistories)(struct SymTriggerRouterService *this);
	SymList * (*getActiveTriggerHistoriesForTable)(struct SymTriggerRouterService *this, char *tableName);
	SymList* (*buildTriggersForSymmetricTables)(struct SymTriggerRouterService *this, SymStringArray *tablesToExclude);
	SymTrigger* (*buildTriggerForSymmetricTable)(struct SymTriggerRouterService *this, char *tableName);
	SymList* (*buildTriggerRoutersForSymmetricTables)(struct SymTriggerRouterService *this,
			char *version, SymNodeGroupLink *nodeGroupLink, SymStringArray *tablesToExclude);
	char* (*buildSymmetricTableRouterId)(struct SymTriggerRouterService *this,
			char *triggerId, char *sourceNodeGroupdId, char *targetNodeGroupId);
	SymTriggerRouter * (*buildTriggerRoutersForSymmetricTablesWithNodeGroupLink)(struct SymTriggerRouterService *this,
	        char *version, SymTrigger *trigger, SymNodeGroupLink *nodeGroupLink);
	SymList * (*getTriggerRouterForTableForCurrentNode)(struct SymTriggerRouterService *this, SymNodeGroupLink *link, char *catalogName, char *schemaName, char *tableName, unsigned short refreshCache);
	unsigned short (*isMatch)(struct SymTriggerRouterService *this, SymNodeGroupLink *link, SymTriggerRouter *router);
	unsigned short (*isMatchTableName)(struct SymTriggerRouterService *this, char *catalogName, char *schemaName, char *tableName, SymTrigger *trigger);
	SymList * (*getConfigurationTablesTriggerRoutersForCurrentNode)(struct SymTriggerRouterService *this, char *sourceNodeGroupId);
	void (*mergeInConfigurationTablesTriggerRoutersForCurrentNode)(struct SymTriggerRouterService *this, char *sourceNodeGroupId, SymList *configuredInDatabase);
	unsigned short (*doesTriggerRouterExistInList)(struct SymTriggerRouterService *this, SymList *triggerRouters, SymTriggerRouter *triggerRouter);
	SymTriggerRouter * (*getTriggerRouterForCurrentNode)(struct SymTriggerRouterService *this, char *triggerId, char *routerId, unsigned short refreshCache);
	SymMap * (*getTriggerRoutersForCurrentNode)(struct SymTriggerRouterService *this, unsigned short refreshCache);
	SymList * (*getTriggersForCurrentNode)(struct SymTriggerRouterService *this, unsigned short refreshCache);
	SymTriggerRoutersCache * (*getTriggerRoutersCacheForCurrentNode)(struct SymTriggerRouterService *this, unsigned short refreshCache);
	SymRouter * (*getActiveRouterByIdForCurrentNode)(struct SymTriggerRouterService *this, char *routerId, unsigned short refreshCache);
	SymList * (*getRoutersByGroupLink)(struct SymTriggerRouterService *this, SymNodeGroupLink *link);
	SymTrigger * (*getTriggerForCurrentNodeById)(struct SymTriggerRouterService *this, char *triggerId);
	SymTrigger * (*getTriggerById)(struct SymTriggerRouterService *this, char *triggerId, unsigned short refreshCache);
	SymRouter * (*getRouterById)(struct SymTriggerRouterService *this, char *routerId, unsigned short refreshCache);
	SymList * (*getRouters)(struct SymTriggerRouterService *this, unsigned short replaceVariables);
	char * (*getTriggerRouterSql)(struct SymTriggerRouterService *this, char *sql);
	SymList * (*getTriggerRouters)(struct SymTriggerRouterService *this, unsigned short refreshCache);
	SymList * (*getAllTriggerRoutersForCurrentNode)(struct SymTriggerRouterService *this, char *sourceNodeGroupId);
	SymList * (*getAllTriggerRoutersForReloadForCurrentNode)(struct SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId);
	SymTriggerRouter * (*findTriggerRouterById)(struct SymTriggerRouterService *this, char *triggerId, char *routerId);
	SymList * (*enhanceTriggerRouters)(struct SymTriggerRouterService *this, SymList *triggerRouters);
	SymMap * (*getTriggerRoutersByChannel)(struct SymTriggerRouterService *this, char *nodeGroupId, unsigned short refreshCache);
	void (*insert)(struct SymTriggerRouterService *this, SymTriggerHistory *newHistRecord);
	void (*deleteTriggerRouterWithId)(struct SymTriggerRouterService *this, char *triggerId, char *routerId);
	void (*deleteTriggerRouter)(struct SymTriggerRouterService *this, SymTriggerRouter *triggerRouter);
	void (*saveTriggerRouter)(struct SymTriggerRouterService *this, SymTriggerRouter *triggerRouter, unsigned short updateTriggerRouterTableOnly);
	void (*resetTriggerRouterCacheByNodeGroupId)(struct SymTriggerRouterService *this);
	void (*saveRouter)(struct SymTriggerRouterService *this, SymRouter *router);
	unsigned short (*isRouterBeingUsed)(struct SymTriggerRouterService *this, char *routerId);
	void (*deleteRouter)(struct SymTriggerRouterService *this, SymRouter *router);
	void (*saveTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger);
	void (*clearCache)(struct SymTriggerRouterService *this);
	SymList * (*getTriggerIdsFrom)(struct SymTriggerRouterService *this, SymList *triggersThatShouldBeActive);
	SymTrigger * (*getTriggerFromList)(struct SymTriggerRouterService *this, char *triggerId, SymList *triggersThatShouldBeActive);
	void (*inactivateTriggers)(struct SymTriggerRouterService *this, SymList *triggersThatShouldBeActive, SymStringBuilder *sqlBuffer, SymList *activeTriggerHistories);
	unsigned short (*isEqual)(struct SymTriggerRouterService *this, char *one, char *two, unsigned short ignoreCase);
	void (*dropTriggersForTriggerHistory)(struct SymTriggerRouterService *this, SymTriggerHistory *history, SymStringBuilder *sqlBuffer);
	SymList * (*toList)(struct SymTriggerRouterService *this, SymList *source);
	SymList * (*getTablesForTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger, SymList *triggers, unsigned short useTableCache);
	unsigned short (*containsExactMatchForSourceTableName)(struct SymTriggerRouterService *this, SymTable *table, SymList *triggers, unsigned short ignoreCase);
	void (*updateOrCreateDatabaseTriggers)(struct SymTriggerRouterService *this, SymList *triggers, SymStringBuilder *sqlBuffer, unsigned short force, unsigned short verifyInDatabase, SymList *activeTriggerHistories, unsigned short useTableCache);
	void (*updateOrCreateDatabaseTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger, SymList *triggers, SymStringBuilder *sqlBuffer, unsigned short force, unsigned short verifyInDatabase, SymList *activeTriggerHistories, unsigned short useTableCache);
	void (*syncTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger, unsigned short force, unsigned short verifyInDatabase);
	void (*updateOrCreateDatabaseTriggersForTable)(struct SymTriggerRouterService *this, SymTrigger *trigger, SymTable *table, SymStringBuilder *sqlBuffer, unsigned short force, unsigned short verifyInDatabase, SymList *activeTriggerHistories);
	SymTriggerHistory * (*rebuildTriggerIfNecessary)(struct SymTriggerRouterService *this, SymStringBuilder *sqlBuffer, unsigned short forceRebuild, SymTrigger *trigger, SymDataEventType *dmlType, char *reason, SymTriggerHistory *oldhist, SymTriggerHistory *hist, unsigned short triggerIsActive, SymTable *table, SymList *activeTriggerHistories);
	char * (*replaceCharsToShortenName)(struct SymTriggerRouterService *this, char *triggerName);
	char * (*getTriggerName)(struct SymTriggerRouterService *this, SymDataEventType *dml, int maxTriggerNameLength, SymTrigger *trigger, SymTable *table, SymList *activeTriggerHistories);
	SymTriggerHistory * (*mapRow)(struct SymTriggerRouterService *this, SymRow *rs);
	SymNodeGroupLink * (*getNodeGroupLink)(struct SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId);
//	SymRouter * (*mapRow)(struct SymTriggerRouterService *this, SymRow *rs);
//	SymTrigger * (*mapRow)(struct SymTriggerRouterService *this, SymRow *rs);
//	SymTriggerRouter * (*mapRow)(struct SymTriggerRouterService *this, SymRow *rs);
	void (*addExtraConfigTable)(struct SymTriggerRouterService *this, char *table);
	SymMap * (*getFailedTriggers)(struct SymTriggerRouterService *this);
	SymTriggerHistory * (*findTriggerHistoryForGenericSync)(struct SymTriggerRouterService *this);
	SymMap * (*fillTriggerRoutersByHistIdAndSortHist)(struct SymTriggerRouterService *this, char *sourceNodeGroupId, char *targetNodeGroupId, SymList *triggerHistories);
	SymList * (*getSortedTablesFor)(struct SymTriggerRouterService *this, SymList *histories);
	int (*syncTriggers)(struct SymTriggerRouterService *this, SymStringBuilder *sqlBuffer, unsigned short force); //
    int (*syncTriggersWithTable)(struct SymTriggerRouterService *this, SymTable *table, unsigned short force); //
    void (*destroy)(struct SymTriggerRouterService *this); //
} SymTriggerRouterService;

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService * this, SymParameterService *parameterService, SymDatabasePlatform *platform, SymConfigurationService *configurationService);

#endif
