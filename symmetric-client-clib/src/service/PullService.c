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

// TODO: should be SymRemoteNodeStatuses

SymRemoteNodeStatus * SymPullService_pull_data(SymPullService *this) {
    SymNode *identity = this->nodeService->find_identity(this->nodeService);
    if (identity == NULL) {
        this->registrationService->register_with_server(this->registrationService);
        identity = this->nodeService->find_identity(this->nodeService);
    }
    if (identity->syncEnabled) {
        SymNode **nodes = this->nodeService->find_nodes_to_pull(this->nodeService);
        int index = 0;
        for (; nodes[index] != NULL; index++) {
            SymNode *node = nodes[index];
            SymRemoteNodeStatus *status = SymRemoteNodeStatus_new(NULL);
            status->nodeId = node->nodeId;

            int pullCount = 0;
            long batchesProcessedCount = 0;
            do {
                batchesProcessedCount = status->batchesProcessed;
                pullCount++;

                printf("Pull requested for %s:%s:%s\n", node->nodeGroupId, node->externalId, node->nodeId);
                if (pullCount > 1) {
                    printf("Immediate pull requested while in reload mode\n");
                }

                this->dataLoaderService->load_data_from_pull(this->dataLoaderService, node, status);

                if (!status->failed && (status->dataProcessed > 0 || status->batchesProcessed > 0)) {
                    printf("Pull data received from %s:%s:%s.  %lu rows and %lu batches were processed\n",
                            node->nodeGroupId, node->externalId, node->nodeId, status->dataProcessed, status->batchesProcessed);
                } else if (status->failed) {
                    printf("There was a failure while pulling data from %s:%s:%s.  %lu rows and %lu batches were processed\n",
                            node->nodeGroupId, node->externalId, node->nodeId, status->dataProcessed, status->batchesProcessed);
                }
            } while (this->nodeService->is_dataload_started(this->nodeService) && !status->failed
                    && status->batchesProcessed > batchesProcessedCount);
        }
    }
    identity->destroy(identity);
    return NULL;
}

void SymPullService_destroy(SymPullService *this) {
    free(this);
}

SymPullService * SymPullService_new(SymPullService *this, SymNodeService *nodeService, SymDataLoaderService *dataLoaderService,
        SymRegistrationService *registrationService) {
    if (this == NULL) {
        this = (SymPullService *) calloc(1, sizeof(SymPullService));
    }
    this->nodeService = nodeService;
    this->dataLoaderService = dataLoaderService;
    this->registrationService = registrationService;
    this->pull_data = (void *) &SymPullService_pull_data;
    this->destroy = (void *) &SymPullService_destroy;
    return this;
}
