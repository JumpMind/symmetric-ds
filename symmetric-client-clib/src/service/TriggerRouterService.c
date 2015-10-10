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

void SymTriggerRouterService_clearCache(SymTriggerRouterService *this) {
    this->triggerRouterPerNodeCacheTime = 0;
    this->triggerRouterPerChannelCacheTime = 0;
    this->triggerRoutersCacheTime = 0;
    this->routersCacheTime = 0;
    this->triggersCacheTime = 0;
}

SymMap* SymTriggerRouterService_getTriggerRoutersForCurrentNode(SymTriggerRouterService *this, unsigned short refreshCache) {
	return this->getTriggerRoutersCacheForCurrentNode(this, refreshCache)->triggerRoutersByTriggerId;
}

SymList* SymTriggerRouterService_getTriggersForCurrentNode(SymTriggerRouterService *this, unsigned short refreshCache) {

	//	SymTriggerSelector *triggerSelector = SymTriggerSelector_new(NULL, NULL /** todo */);
	//	return triggerSelector->select(triggerSelector);
//	SymMap *triggerRouters = this->getTriggerRoutersCacheForCurrentNode(this, refreshCache);
//	SymList *triggers = SymList_new(NULL);
	// TODO >>>>>>>>

	return NULL;
}


SymTriggerRoutersCache* SymTriggerRouterService_getTriggerRoutersCacheForCurrentNode(SymTriggerRouterService *this, unsigned short refreshCache) {
	char* myNodeGroupId = this->parameterService->getNodeGroupId(this->parameterService);
	long triggerRouterCacheTimeoutInMs = this->parameterService->getLong(this->parameterService, CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS, 1000);
	SymTriggerRoutersCache *cache = this->triggerRouterCacheByNodeGroupId == NULL ? NULL
			: this->triggerRouterCacheByNodeGroupId->get(this->triggerRouterCacheByNodeGroupId, myNodeGroupId);

	// TODO determine a way to get time in ms. to expire the cache.
	// clock_t timeInMilliseconds = clock() / (CLOCKS_PER_SEC / 1000);
//	clock_t timeInSeconds = clock()/CLOCKS_PER_SEC;
//	printf("%ul", time);


	if (cache == NULL
			|| refreshCache
			/** TODO check timeInMilliseconds */) {
		// TODO >>>>>>>>
	}

	return NULL;
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
    this->syncTriggers = (void *) &SymTriggerRouterService_syncTriggers;
    this->syncTriggersWithTable = (void *) &SymTriggerRouterService_syncTriggersWithTable;
    this->getTriggerRoutersForCurrentNode = (void *) &SymTriggerRouterService_getTriggerRoutersForCurrentNode;
    this->getTriggersForCurrentNode = (void *) &SymTriggerRouterService_getTriggersForCurrentNode;
    this->getTriggerRoutersCacheForCurrentNode = (void *)SymTriggerRouterService_getTriggerRoutersCacheForCurrentNode;
    this->clearCache = (void *) &SymTriggerRouterService_clearCache;
    this->destroy = (void *) &SymTriggerRouterService_destroy;
    return this;
}
