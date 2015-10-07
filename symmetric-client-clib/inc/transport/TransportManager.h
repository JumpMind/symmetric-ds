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
#ifndef SYM_TRANSPORT_MANAGER_H
#define SYM_TRANSPORT_MANAGER_H

#include <stdio.h>
#include <stdlib.h>
#include <util/Properties.h>
#include "service/ParameterService.h"
#include "model/BatchAck.h"
#include "model/IncomingBatch.h"
#include "model/Node.h"
#include "transport/IncomingTransport.h"
#include "transport/OutgoingTransport.h"
#include "util/List.h"

typedef struct SymTransportManager {
    SymParameterService *parameterService;
    int (*sendAcknowledgement)(struct SymTransportManager *this, SymNode *remote, SymList *batches, SymNode *local, char *securityToken, char *registrationUrl);
    SymBatchAck ** (*readAcknowledgement)(struct SymTransportManager *this, char *parameterString1, char *parameterString2);
    SymIncomingTransport * (*getPullTransport)(struct SymTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl);
    SymOutgoingTransport * (*getPushTransport)(struct SymTransportManager *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl);
    SymIncomingTransport * (*getRegisterTransport)(struct SymTransportManager *this, SymNode *local, char *registrationUrl);
    void (*destroy)(struct SymTransportManager *this);
} SymTransportManager;

#endif
