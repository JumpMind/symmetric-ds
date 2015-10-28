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
#include "model/RemoteNodeStatus.h"

void SymRemoteNodeStatus_updateIncomingStatus(SymRemoteNodeStatus *this, SymList *incomingBatches) {
    SymIterator *iter = incomingBatches->iterator(incomingBatches);
    while (iter->hasNext(iter)) {
        SymIncomingBatch *incomingBatch = (SymIncomingBatch *) iter->next(iter);
        this->dataProcessed += incomingBatch->statementCount;
        this->batchesProcessed++;
        if (strcmp(incomingBatch->status, SYM_INCOMING_BATCH_STATUS_ERROR) == 0) {
            this->status = SYM_REMOTE_NODE_STATUS_DATA_ERROR;
        }
    }

    if (this->status != SYM_REMOTE_NODE_STATUS_DATA_ERROR && this->dataProcessed > 0) {
        this->status = SYM_REMOTE_NODE_STATUS_DATA_PROCESSED;
    }
}

void SymRemoteNodeStatus_updateOutgoingStatus(SymRemoteNodeStatus *this, SymList *outgoingBatches, SymList *batchAcks) {
    if (batchAcks) {
        SymIterator *iter = batchAcks->iterator(batchAcks);
        while (iter->hasNext(iter)) {
            SymBatchAck *batchAck = (SymBatchAck *) iter->next(iter);
            if (! batchAck->isOk) {
                this->status = SYM_REMOTE_NODE_STATUS_DATA_ERROR;
            }
        }
        iter->destroy(iter);
    }

    if (outgoingBatches) {
        SymIterator *iter = outgoingBatches->iterator(outgoingBatches);
        while (iter->hasNext(iter)) {
            SymOutgoingBatch *batch = (SymOutgoingBatch *) iter->next(iter);
            this->batchesProcessed++;
            this->dataProcessed += batch->totalEventCount(batch);
            SymChannel *channel = this->channels->get(this->channels, batch->channelId);
            if (channel && channel->reloadFlag) {
                this->reloadBatchesProcessed++;
            }

            if (strcmp(batch->status, SYM_OUTGOING_BATCH_ERROR) == 0) {
                this->status = SYM_REMOTE_NODE_STATUS_DATA_ERROR;
            }
        }
    }

    if (this->status != SYM_REMOTE_NODE_STATUS_DATA_ERROR && this->dataProcessed > 0) {
        this->status = SYM_REMOTE_NODE_STATUS_DATA_PROCESSED;
    }
}

void SymRemoteNodeStatus_destroy(SymRemoteNodeStatus *this) {
    free(this);
}

SymRemoteNodeStatus * SymRemoteNodeStatus_new(SymRemoteNodeStatus *this, char *nodeId, SymMap *channels) {
    if (this == NULL) {
        this = (SymRemoteNodeStatus *) calloc(1, sizeof(SymRemoteNodeStatus));
    }
    this->nodeId = nodeId;
    this->channels = channels;
    this->destroy = (void *) &SymRemoteNodeStatus_destroy;
    this->updateIncomingStatus = (void *) &SymRemoteNodeStatus_updateIncomingStatus;
    this->updateOutgoingStatus = (void *) &SymRemoteNodeStatus_updateOutgoingStatus;
    return this;
}
