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
#include "service/PushService.h"

SymList * SymPushService_readAcks(SymList *batches, SymOutgoingTransport *transport, SymTransportManager *transportManager, SymAcknowledgeService *acknowledgeService) {
    SymList *batchIds = SymList_new(NULL);
    SymIterator *iter = batches->iterator(batches);
    while (iter->hasNext(iter)) {
        SymOutgoingBatch *outgoingBatch = (SymOutgoingBatch *) iter->next(iter);
        if (SymStringUtils_equals(outgoingBatch->status, SYM_OUTGOING_BATCH_LOADING)) {
            batchIds->add(batchIds, SymStringUtils_format("%ld", outgoingBatch->batchId));
        }
    }
    iter->destroy(iter);

    SymList *batchAcks = transportManager->readAcknowledgement(transportManager, transport->ackString, transport->ackExtendedString);

    long batchIdInError = LONG_MAX;
    iter = batchAcks->iterator(batchAcks);
    while (iter->hasNext(iter)) {
        SymBatchAck *batchAck = (SymBatchAck *) iter->next(iter);
        char *batchId = SymStringUtils_format("%ld", batchAck->batchId);
        free(batchIds->removeObject(batchIds, batchId, (void *) strcmp));
        free(batchId);
        if (!batchAck->isOk) {
            batchIdInError = batchAck->batchId;
        }
        SymLog_debug("Saving ack: nodeId=%s, batchId=%ld, status=%s", batchAck->nodeId, batchAck->batchId,
                (batchAck->isOk ? SYM_OUTGOING_BATCH_OK : SYM_OUTGOING_BATCH_ERROR));
        acknowledgeService->ack(acknowledgeService, batchAck);
    }
    iter->destroy(iter);

    iter = batchIds->iterator(batchIds);
    while (iter->hasNext(iter)) {
        char *batchId = (char *) iter->next(iter);
        if (atol(batchId) < batchIdInError) {
            SymLog_error("We expected but did not receive an ack for batch %s", batchId);
        }
    }
    iter->destroy(iter);
    batchIds->destroyAll(batchIds, (void *) free);
    return batchAcks;
}

void SymPushService_pushToNode(SymPushService *this, SymNode *remote, SymRemoteNodeStatus *status) {
    SymNode *identity = this->nodeService->findIdentityWithCache(this->nodeService, 0);
    SymNodeSecurity *identitySecurity = this->nodeService->findNodeSecurity(this->nodeService, identity->nodeId);
    SymOutgoingTransport *transport = this->transportManager->getPushTransport(this->transportManager, remote, identity, identitySecurity->nodePassword,
            this->parameterService->getRegistrationUrl(this->parameterService));
    SymList *extractedBatches = this->dataExtractorService->extract(this->dataExtractorService, remote, transport);
    if (extractedBatches->size > 0) {
        SymLog_info("Push data sent to %s:%s:%s", remote->nodeGroupId, remote->externalId, remote->nodeId);
        SymList *batchAcks = this->readAcks(extractedBatches, transport, this->transportManager, this->acknowledgeService);
        status->updateOutgoingStatus(status, extractedBatches, batchAcks);
        batchAcks->destroy(batchAcks);
    }
    extractedBatches->destroy(extractedBatches);
    transport->destroy(transport);
    identitySecurity->destroy(identitySecurity);
}

void SymPushService_execute(SymPushService *this, SymNode *node, SymRemoteNodeStatus *status) {
    long reloadBatchesProcessed = 0;
    long lastBatchCount = 0;
    do {
        if (lastBatchCount > 0) {
            SymLog_info("Pushing to %s:%s:%s again because the last push contained reload batches",
                    node->nodeGroupId, node->externalId, node->nodeId);
        }
        reloadBatchesProcessed = status->reloadBatchesProcessed;
        SymLog_debug("Push requested for %s:%s:%s", node->nodeGroupId, node->externalId, node->nodeId);
        SymPushService_pushToNode(this, node, status);
        if (!status->failed && status->batchesProcessed > 0
                && status->batchesProcessed != lastBatchCount) {
            SymLog_info("Pushed data to %s:%s:%s. %ld data and %ld batches were processed",
                    node->nodeGroupId, node->externalId, node->nodeId, status->dataProcessed, status->batchesProcessed);
        } else if (status->failed) {
            SymLog_info("There was a failure while pushing data to %s:%s:%s. {} data and {} batches were processed",
                    node->nodeGroupId, node->externalId, node->nodeId, status->dataProcessed, status->batchesProcessed);
        }
        SymLog_debug("Push completed for %s:%s:%s", node->nodeGroupId, node->externalId, node->nodeId);
        lastBatchCount = status->batchesProcessed;
    } while (status->reloadBatchesProcessed > reloadBatchesProcessed && !status->failed);
}

SymRemoteNodeStatuses * SymPushService_pushData(SymPushService *this, unsigned int force) {
    SymMap *channels = this->configurationService->getChannels(this->configurationService, 0);
    SymRemoteNodeStatuses *statuses = SymRemoteNodeStatuses_new(NULL, channels);
    SymNode *identity = this->nodeService->findIdentity(this->nodeService);
    if (identity && identity->syncEnabled) {
        SymList *nodes = this->nodeCommunicationService->list(this->nodeCommunicationService, SYM_COMMUNICATION_TYPE_PUSH);
        if (nodes->size > 0) {
            SymNodeSecurity *identitySecurity = this->nodeService->findNodeSecurity(this->nodeService, identity->nodeId);
            if (identitySecurity) {
                SymIterator *iter = nodes->iterator(nodes);
                while (iter->hasNext(iter)) {
                    SymNode *node = (SymNode *) iter->next(iter);
                    SymRemoteNodeStatus *status = statuses->add(statuses, node->nodeId);
                    SymPushService_execute(this, node, status);
                }
                identitySecurity->destroy(identitySecurity);
                iter->destroy(iter);
            } else {
                SymLog_error("Could not find a node security row for '%s'.  A node needs a matching security row in both the local and remote nodes if it is going to authenticate to push data",
                        identity->nodeId);
            }
        }
        nodes->destroy(nodes);
    }
    channels->destroyAll(channels, (void *)SymChannel_destroy);
    return statuses;
}

void SymPushService_destroy(SymPushService *this) {
    free(this);
}

SymPushService * SymPushService_new(SymPushService *this, SymNodeService *nodeService, SymDataExtractorService *dataExtractorService,
        SymTransportManager *transportManager, SymParameterService *parameterService, SymConfigurationService *configurationService,
        SymAcknowledgeService *acknowledgeService, SymNodeCommunicationService *nodeCommunicationService) {
    if (this == NULL) {
        this = (SymPushService *) calloc(1, sizeof(SymPushService));
    }
    this->nodeService = nodeService;
    this->dataExtractorService = dataExtractorService;
    this->transportManager = transportManager;
    this->parameterService = parameterService;
    this->configurationService = configurationService;
    this->acknowledgeService = acknowledgeService;
    this->nodeCommunicationService = nodeCommunicationService;
    this->pushData = (void *) &SymPushService_pushData;
    this->readAcks = (void *) &SymPushService_readAcks;
    this->destroy = (void *) &SymPushService_destroy;
    return this;
}
