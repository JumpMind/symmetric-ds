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
#include "service/PullService.h"

void SymPullService_execute(SymPullService *this, SymNode *node, SymRemoteNodeStatus *status) {
    int pullCount = 0;
    long batchesProcessedCount = 0;
    do {
        batchesProcessedCount = status->batchesProcessed;
        pullCount++;

        SymLog_debug("Pull requested for %s:%s:%s", node->nodeGroupId, node->externalId, node->nodeId);
        if (pullCount > 1) {
            SymLog_info("Immediate pull requested while in reload mode\n");
        }

        this->dataLoaderService->loadDataFromPull(this->dataLoaderService, node, status);

        if (!status->failed && (status->dataProcessed > 0 || status->batchesProcessed > 0)) {
            SymLog_info("Pull data received from %s:%s:%s.  %lu rows and %lu batches were processed",
                    node->nodeGroupId, node->externalId, node->nodeId, status->dataProcessed, status->batchesProcessed);
        } else if (status->failed) {
            SymLog_info("There was a failure while pulling data from %s:%s:%s.  %lu rows and %lu batches were processed",
                    node->nodeGroupId, node->externalId, node->nodeId, status->dataProcessed, status->batchesProcessed);
        }
    } while (this->nodeService->isDataloadStarted(this->nodeService) && !status->failed
            && status->batchesProcessed > batchesProcessedCount);
}

SymRemoteNodeStatuses * SymPullService_pullData(SymPullService *this) {
    SymRemoteNodeStatuses *statuses = NULL;
    SymNode *identity = this->nodeService->findIdentity(this->nodeService);
    if (identity == NULL) {
        this->registrationService->registerWithServer(this->registrationService);
        identity = this->nodeService->findIdentity(this->nodeService);
    }
    if (identity->syncEnabled) {
        SymList *nodes = this->nodeCommunicationService->list(this->nodeCommunicationService, SYM_COMMUNICATION_TYPE_PULL);
        SymMap *channels = this->configurationService->getChannels(this->configurationService, 0);
        statuses = SymRemoteNodeStatuses_new(NULL, channels);
        SymIterator *iter = nodes->iterator(nodes);
        while (iter->hasNext(iter)) {
            SymNode *node = (SymNode *) iter->next(iter);
            SymRemoteNodeStatus *status = statuses->add(statuses, node->nodeId);
            SymPullService_execute(this, node, status);
        }
        channels->destroyAll(channels, (void *)SymChannel_destroy);
        iter->destroy(iter);
        nodes->destroy(nodes);
    }
    return statuses;
}

void SymPullService_destroy(SymPullService *this) {
    free(this);
}

SymPullService * SymPullService_new(SymPullService *this, SymNodeService *nodeService, SymDataLoaderService *dataLoaderService,
        SymRegistrationService *registrationService, SymConfigurationService *configurationService,
        SymNodeCommunicationService *nodeCommunicationService) {
    if (this == NULL) {
        this = (SymPullService *) calloc(1, sizeof(SymPullService));
    }
    this->nodeService = nodeService;
    this->dataLoaderService = dataLoaderService;
    this->registrationService = registrationService;
    this->configurationService = configurationService;
    this->nodeCommunicationService = nodeCommunicationService;
    this->pullData = (void *) &SymPullService_pullData;
    this->destroy = (void *) &SymPullService_destroy;
    return this;
}
