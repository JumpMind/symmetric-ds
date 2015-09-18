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
#ifndef SYM_DATA_LOADER_SERVICE_H
#define SYM_DATA_LOADER_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include "service/NodeService.h"
#include "service/ParameterService.h"
#include "transport/TransportManager.h"
#include "transport/IncomingTransport.h"
#include "model/Node.h"
#include "model/RemoteNodeStatus.h"
#include "io/reader/ProtocolDataReader.h"
#include "io/writer/DefaultDatabaseWriter.h"
#include "db/DatabasePlatform.h"

typedef struct {
    SymParameterService *parameterService;
    SymNodeService *nodeService;
    SymTransportManager *transportManager;
    SymDatabasePlatform *platform;
    void (*load_data_from_pull)(void *this, SymNode *remote, SymRemoteNodeStatus *status);
    void (*load_data_from_registration)(void *this, SymRemoteNodeStatus *status);
    void (*destroy)(void *this);
} SymDataLoaderService;

SymDataLoaderService * SymDataLoaderService_new(SymDataLoaderService *this, SymParameterService *parameterService,
        SymNodeService *nodeService, SymTransportManager *transportManager, SymDatabasePlatform *platform);

#endif
