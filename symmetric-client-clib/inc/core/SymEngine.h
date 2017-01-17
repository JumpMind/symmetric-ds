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
#ifndef SYM_ENGINE_H
#define SYM_ENGINE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "common/ParameterConstants.h"
#include "common/Log.h"
#include "db/platform/DatabasePlatformFactory.h"
#include "db/SymDialectFactory.h"
#include "db/SymDialect.h"
#include "db/sqlite/SqliteDialect.h"
#include "service/AcknowledgeService.h"
#include "service/TriggerRouterService.h"
#include "service/ParameterService.h"
#include "service/PushService.h"
#include "service/NodeService.h"
#include "service/NodeCommunicationService.h"
#include "service/PullService.h"
#include "service/RegistrationService.h"
#include "service/RouterService.h"
#include "service/DataLoaderService.h"
#include "service/DataService.h"
#include "service/DataExtractorService.h"
#include "service/IncomingBatchService.h"
#include "service/OutgoingBatchService.h"
#include "service/ConfigurationService.h"
#include "transport/TransportManagerFactory.h"
#include "transport/TransportManager.h"
#include "model/RemoteNodeStatuses.h"
#include "util/Properties.h"
#include "common/Constants.h"
#include "common/ParameterConstants.h"
#include "service/PurgeService.h"
#include "service/OfflinePullService.h"
#include "service/OfflinePushService.h"
#include "service/FileSyncService.h"

typedef struct SymEngine {
    SymProperties *properties;
    SymDialect *dialect;
    SymDatabasePlatform *platform;
    SymParameterService *parameterService;
    SymTransportManager *transportManager;
    SymTransportManager *offlineTransportManager;
    SymTriggerRouterService *triggerRouterService;
    SymDataLoaderService *dataLoaderService;
    SymDataService *dataService;
    SymRouterService *routerService;
    SymDataExtractorService *dataExtractorService;
    SymRegistrationService *registrationService;
    SymPushService *pushService;
    SymPullService *pullService;
    SymOfflinePushService *offlinePushService;
    SymOfflinePullService *offlinePullService;
    SymNodeService *nodeService;
    SymNodeCommunicationService *nodeCommunicationService;
    SymIncomingBatchService *incomingBatchService;
    SymOutgoingBatchService *outgoingBatchService;
    SymAcknowledgeService *acknowledgeService;
    SymConfigurationService *configurationService;
    SymSequenceService *sequenceService;
    SymPurgeService *purgeService;
    SymFileSyncService *fileSyncService;

    unsigned short (*start)(struct SymEngine *this);
    unsigned short (*stop)(struct SymEngine *this);
    unsigned short (*uninstall)(struct SymEngine *this);
    void (*syncTriggers)(struct SymEngine *this);
    SymRemoteNodeStatuses * (*push)(struct SymEngine *this);
    SymRemoteNodeStatuses * (*pull)(struct SymEngine *this);
    void (*route)(struct SymEngine *this);
    void (*purge)(struct SymEngine *this);
    void (*heartbeat)(struct SymEngine *this, unsigned short force);
    void (*destroy)(struct SymEngine *this);
} SymEngine;

SymEngine * SymEngine_new(SymEngine *this, SymProperties *properties);

#endif
