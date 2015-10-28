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
#ifndef SYM_ROUTER_SERVICE_H
#define SYM_ROUTER_SERVICE_H

#include <stdio.h>
#include <time.h>
#include "common/Constants.h"
#include "route/ChannelRouterContext.h"
#include "route/DataRouter.h"
#include "route/DefaultDataRouter.h"
#include "route/DataGapRouteReader.h"
#include "db/platform/DatabasePlatform.h"
#include "service/NodeService.h"
#include "service/ConfigurationService.h"
#include "service/OutgoingBatchService.h"
#include "service/SequenceService.h"
#include "service/TriggerRouterService.h"
#include "service/DataService.h"
#include "service/ParameterService.h"
#include "model/Channel.h"
#include "model/Router.h"
#include "model/Data.h"
#include "model/DataMetaData.h"
#include "model/OutgoingBatch.h"
#include "util/List.h"
#include "util/Map.h"
#include "util/StringUtils.h"

#define SYM_ROUTER_DEFAULT "default"

typedef struct SymRouterService {
    SymOutgoingBatchService *outgoingBatchService;
    SymSequenceService *sequenceService;
    SymDataService *dataService;
    SymNodeService *nodeService;
    SymConfigurationService *configurationService;
    SymTriggerRouterService *triggerRouterService;
    SymParameterService *parameterService;
    SymDatabasePlatform *platform;
    SymMap *routers;
    long (*routeData)(struct SymRouterService *this);
    void (*destroy)(struct SymRouterService *this);
} SymRouterService;

SymRouterService * SymRouterService_new(SymRouterService *this, SymOutgoingBatchService *outgoingBatchService, SymSequenceService *sequenceService,
        SymDataService *dataService, SymNodeService *nodeService, SymConfigurationService *configurationService, SymParameterService *parameterService,
        SymTriggerRouterService *triggerRouterService, SymDatabasePlatform *platform);

#endif
