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
#ifndef SYM_NODECOMMUNICATIONSERVICE_H
#define SYM_NODECOMMUNICATIONSERVICE_H

#include <stdlib.h>
#include "util/List.h"
#include "service/ParameterService.h"
#include "service/NodeService.h"

typedef enum {
    SYM_COMMUNICATION_TYPE_PULL, SYM_COMMUNICATION_TYPE_PUSH, SYM_COMMUNICATION_TYPE_FILE_PUSH, SYM_COMMUNICATION_TYPE_FILE_PULL,
    SYM_COMMUNICATION_TYPE_OFFLN_PULL, SYM_COMMUNICATION_TYPE_OFFLN_PUSH, SYM_COMMUNICATION_TYPE_EXTRACT,
    SYM_COMMUNICATION_TYPE_OFF_FSPULL, SYM_COMMUNICATION_TYPE_OFF_FSPUSH
} SymCommunicationType;

typedef struct SymNodeCommunicationService {
    SymNodeService *nodeService;
    SymParameterService *parameterService;
    SymList * (*list)(struct SymNodeCommunicationService *this, SymCommunicationType communicationType);
    void (*destroy)(struct SymNodeCommunicationService *this);
} SymNodeCommunicationService;

SymNodeCommunicationService * SymNodeCommunicationService_new(SymNodeCommunicationService *this,
        SymNodeService * nodeService, SymParameterService *parameterService);

#endif
