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
    if (this->parameterService->is(this->parameterService, SYM_PARAMETER_NODE_OFFLINE, 0)) {
        nodes->resetAll(nodes, NULL);
    } else {
        SymList *parms = this->parameterService->getDatabaseParametersFor(this->parameterService, SYM_PARAMETER_NODE_OFFLINE);
        int i;
        for (i = 0; i < parms->size; ++i) {
            SymDatabaseParameter *parm = parms->get(parms, i);
            int j;
            for (j = 0; j < nodes->size; ++j) {
                SymNode *node = nodes->get(nodes, j);
                if ((SymStringUtils_equals(node->nodeGroupId, SYM_PARAMETER_ALL)
                            || SymStringUtils_equals(node->nodeGroupId, parm->nodeGroupId))
                        && (SymStringUtils_equals(node->externalId, SYM_PARAMETER_ALL)
                                || SymStringUtils_equals(node->externalId, parm->externalId))) {
                    nodes->remove(nodes, j);
                    j--;
                }
            }
        }
    }

    return nodes;
}

SymList * SymNodeCommunicationService_getNodesToCommunicateWithOffline(SymNodeCommunicationService *this, SymCommunicationType communicationType) {

    SymList *nodesToCommunicateWith = NULL;

    if (this->parameterService->is(this->parameterService, SYM_PARAMETER_NODE_OFFLINE, 0)
            || (communicationType == SYM_COMMUNICATION_TYPE_PULL
                    && this->parameterService->is(this->parameterService, SYM_PARAMETER_NODE_OFFLINE_INCOMING_ACCEPT_ALL, 0))) {

        if (communicationType == SYM_COMMUNICATION_TYPE_PUSH) {
            nodesToCommunicateWith = this->nodeService->findTargetNodesFor(this->nodeService, SymNodeGroupLinkAction_W);
            nodesToCommunicateWith->addAll(nodesToCommunicateWith, this->nodeService->findNodesToPushTo(this->nodeService));
        } else if (communicationType == SYM_COMMUNICATION_TYPE_PULL) {
            nodesToCommunicateWith = this->nodeService->findTargetNodesFor(this->nodeService, SymNodeGroupLinkAction_P);
            nodesToCommunicateWith->addAll(nodesToCommunicateWith, this->nodeService->findNodesToPull(this->nodeService));
        }
    }
    else {
        SymList *parms = this->parameterService->getDatabaseParametersFor(this->parameterService, SYM_PARAMETER_NODE_OFFLINE);
        nodesToCommunicateWith = SymList_new(NULL);
        if (parms->size > 0) {
            SymList *sourceNodes = NULL;
            if (communicationType == SYM_COMMUNICATION_TYPE_PUSH) {
                sourceNodes = this->nodeService->findTargetNodesFor(this->nodeService, SymNodeGroupLinkAction_W);
                sourceNodes->addAll(sourceNodes, this->nodeService->findNodesToPushTo(this->nodeService));
            } else if (communicationType == SYM_COMMUNICATION_TYPE_PULL) {
                sourceNodes = this->nodeService->findTargetNodesFor(this->nodeService, SymNodeGroupLinkAction_P);
                sourceNodes->addAll(sourceNodes, this->nodeService->findNodesToPull(this->nodeService));
            }
            if (sourceNodes != NULL && sourceNodes->size > 0) {
                int i;
                for (i = 0; i < parms->size; ++i) {
                    SymDatabaseParameter *parm = parms->get(parms, i);
                    int j;
                    for (j = 0; j < sourceNodes->size; ++j) {
                        SymNode *node = sourceNodes->get(sourceNodes, j);
                        if ((SymStringUtils_equals(node->nodeGroupId, SYM_PARAMETER_ALL)
                                    || SymStringUtils_equals(node->nodeGroupId, parm->nodeGroupId))
                                && (SymStringUtils_equals(node->externalId, SYM_PARAMETER_ALL)
                                        || SymStringUtils_equals(node->externalId, parm->externalId))) {
                            nodesToCommunicateWith->add(nodesToCommunicateWith, node);
                        }
                    }
                }
            }
        }
    }

    return nodesToCommunicateWith;
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
        case SYM_COMMUNICATION_TYPE_OFF_FSPUSH:
        case SYM_COMMUNICATION_TYPE_OFFLN_PUSH:
            nodesToCommunicateWith = SymNodeCommunicationService_getNodesToCommunicateWithOffline(this, SYM_COMMUNICATION_TYPE_PUSH);
            break;
        case SYM_COMMUNICATION_TYPE_OFF_FSPULL:
        case SYM_COMMUNICATION_TYPE_OFFLN_PULL:
            nodesToCommunicateWith = SymNodeCommunicationService_getNodesToCommunicateWithOffline(this, SYM_COMMUNICATION_TYPE_PULL);
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

SymNodeCommunicationService * SymNodeCommunicationService_new(SymNodeCommunicationService *this,
        SymNodeService * nodeService, SymParameterService *parameterService) {
    if (this == NULL) {
        this = (SymNodeCommunicationService *) calloc(1, sizeof(SymNodeCommunicationService));
    }
    this->nodeService = nodeService;
    this->parameterService = parameterService;
    this->list = (void *) &SymNodeCommunicationService_list;
    this->destroy = (void *) &SymNodeCommunicationService_destroy;
    return this;
}
