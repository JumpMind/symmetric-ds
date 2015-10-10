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

	int (*syncTriggers)(struct SymTriggerRouterService *this, SymStringBuilder *sqlBuffer, unsigned short force);
    int (*syncTriggersWithTable)(struct SymTriggerRouterService *this, SymTable *table, unsigned short force);
    void (*clearCache)(struct SymTriggerRouterService *this);
    SymMap* (*getTriggerRoutersForCurrentNode)(struct SymTriggerRouterService *this, unsigned short refreshCache);
    SymList* (*getTriggersForCurrentNode)(struct SymTriggerRouterService *this, unsigned short refreshCache);
    SymTriggerRoutersCache* (*getTriggerRoutersCacheForCurrentNode)(struct SymTriggerRouterService *this, unsigned short refreshCache);
    void (*destroy)(struct SymTriggerRouterService *this);
} SymTriggerRouterService;

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService * this, SymParameterService *parameterService, SymDatabasePlatform *platform, SymConfigurationService *configurationService);

#endif
