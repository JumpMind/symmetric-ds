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
#ifndef SYM_OFFLINEPUSHSERVICE_H
#define SYM_OFFLINEPUSHSERVICE_H

#include <stdlib.h>
#include <stdio.h>
#include <stdlib.h>
#include <limits.h>
#include "model/RemoteNodeStatus.h"
#include "model/RemoteNodeStatuses.h"
#include "service/NodeService.h"
#include "service/ParameterService.h"
#include "service/ConfigurationService.h"
#include "service/DataExtractorService.h"
#include "service/AcknowledgeService.h"
#include "service/NodeCommunicationService.h"
#include "transport/TransportManager.h"
#include "transport/file/FileOutgoingTransport.h"
#include "util/List.h"
#include "util/StringUtils.h"
#include "common/Log.h"

typedef struct SymOfflinePushService {
    SymNodeService *nodeService;
    SymDataExtractorService *dataExtractorService;
    SymTransportManager *transportManager;
    SymParameterService *parameterService;
    SymConfigurationService *configurationService;
    SymAcknowledgeService *acknowledgeService;
    SymNodeCommunicationService *nodeCommunicationService;
    SymRemoteNodeStatuses * (*pushData)(struct SymOfflinePushService *this);
    void (*destroy)(struct SymOfflinePushService *);
} SymOfflinePushService;

SymOfflinePushService * SymOfflinePushService_new(SymOfflinePushService *this, SymNodeService *nodeService,
        SymDataExtractorService *dataExtractorService, SymTransportManager *transportManager,
        SymParameterService *parameterService, SymConfigurationService *configurationService,
        SymAcknowledgeService *acknowledgeService, SymNodeCommunicationService *nodeCommunicationService);

#endif
