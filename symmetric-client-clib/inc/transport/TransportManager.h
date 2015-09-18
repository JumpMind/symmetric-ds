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

#define HTTP_OK 200

typedef struct {
    SymParameterService *parameterService;
    int (*send_acknowledgement)(void *this, SymNode *remote, SymIncomingBatch **batches, SymNode *local, char *securityToken, char *registrationUrl);
    SymBatchAck ** (*read_acknowledgement)(void *this, char *parameterString1, char *parameterString2);
    SymIncomingTransport * (*get_pull_transport)(void *this, SymNode *remote, SymNode *local, char *securityToken, SymProperties *requestProperties, char *registrationUrl);
    SymOutgoingTransport * (*get_push_transport)(void *this, SymNode *remote, SymNode *local, char *securityToken, char *registrationUrl);
    SymIncomingTransport * (*get_register_transport)(void *this, SymNode *local, char *registrationUrl);
    void (*destroy)(void *this);
} SymTransportManager;

#endif
