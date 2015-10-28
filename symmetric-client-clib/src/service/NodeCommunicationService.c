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
#include "service/NodeCommunicationService.h"

SymList * SymNodeCommunicationService_removeOfflineNodes(SymNodeCommunicationService *this, SymList *nodes) {
    // TODO
    return nodes;
}

SymList * SymNodeCommunicationService_getNodesToCommunicateWithOffline(SymNodeCommunicationService *this, SymCommunicationType communicationType) {
    // TODO
    return 0;
}

SymList * SymNodeCommunicationService_list(SymNodeCommunicationService *this, SymCommunicationType communicationType) {
    SymList *nodesToCommunicateWith;

    switch (communicationType) {
        case SYM_COMMUNICATION_TYPE_PULL:
        case SYM_COMMUNICATION_TYPE_FILE_PULL:
            nodesToCommunicateWith = SymNodeCommunicationService_removeOfflineNodes(this, this->nodeService->findNodesToPull(this->nodeService));
            break;
        case SYM_COMMUNICATION_TYPE_FILE_PUSH:
        case SYM_COMMUNICATION_TYPE_PUSH:
            nodesToCommunicateWith = SymNodeCommunicationService_removeOfflineNodes(this, this->nodeService->findNodesToPushTo(this->nodeService));
            break;
        case SYM_COMMUNICATION_TYPE_OFFLN_PUSH:
            nodesToCommunicateWith = SymNodeCommunicationService_getNodesToCommunicateWithOffline(this, SYM_COMMUNICATION_TYPE_FILE_PUSH);
            break;
        case SYM_COMMUNICATION_TYPE_OFFLN_PULL:
            nodesToCommunicateWith = SymNodeCommunicationService_getNodesToCommunicateWithOffline(this, SYM_COMMUNICATION_TYPE_FILE_PULL);
            break;
        default:
            nodesToCommunicateWith = SymList_new(NULL);
            break;
    }

    return nodesToCommunicateWith;
}

void SymNodeCommunicationService_destroy(SymNodeCommunicationService *this) {
    free(this);
}

SymNodeCommunicationService * SymNodeCommunicationService_new(SymNodeCommunicationService *this, SymNodeService * nodeService) {
    if (this == NULL) {
        this = (SymNodeCommunicationService *) calloc(1, sizeof(SymNodeCommunicationService));
    }
    this->nodeService = nodeService;
    this->list = (void *) &SymNodeCommunicationService_list;
    this->destroy = (void *) &SymNodeCommunicationService_destroy;
    return this;
}
