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
#include "model/RemoteNodeStatuses.h"

unsigned int SymRemoteNodeStatuses_wasDataProcessed(SymRemoteNodeStatuses *this) {
    unsigned int dataProcessed = 0;
    SymIterator *iter = this->nodes->iterator(this->nodes);
    while (iter->hasNext(iter)) {
        SymRemoteNodeStatus *status = (SymRemoteNodeStatus *) iter->next(iter);
        dataProcessed |= status->dataProcessed > 0;
    }
    return dataProcessed;
}

unsigned int SymRemoteNodeStatuses_wasBatchProcessed(SymRemoteNodeStatuses *this) {
    unsigned int batchProcessed = 0;
    SymIterator *iter = this->nodes->iterator(this->nodes);
    while (iter->hasNext(iter)) {
        SymRemoteNodeStatus *status = (SymRemoteNodeStatus *) iter->next(iter);
        batchProcessed |= status->batchesProcessed > 0;
    }
    return batchProcessed;
}

long SymRemoteNodeStatuses_getDataProcessedCount(SymRemoteNodeStatuses *this) {
    long dataProcessed = this->nodes->size > 0 ? 0 : -1L;
    SymIterator *iter = this->nodes->iterator(this->nodes);
    while (iter->hasNext(iter)) {
        SymRemoteNodeStatus *status = (SymRemoteNodeStatus *) iter->next(iter);
        dataProcessed += status->dataProcessed;
    }
    return dataProcessed;
}

unsigned int SymRemoteNodeStatuses_errorOccurred(SymRemoteNodeStatuses *this) {
    unsigned int errorOccurred = 0;
    SymIterator *iter = this->nodes->iterator(this->nodes);
    while (iter->hasNext(iter)) {
        SymRemoteNodeStatus *status = (SymRemoteNodeStatus *) iter->next(iter);
        errorOccurred |= status->failed;
    }
    return errorOccurred;
}

SymRemoteNodeStatus * SymRemoteNodeStatuses_add(SymRemoteNodeStatuses *this, char *nodeId) {
    SymRemoteNodeStatus *status = SymRemoteNodeStatus_new(NULL, nodeId, this->channels);
    this->nodes->add(this->nodes, status);
    return status;
}

unsigned int SymRemoteNodeStatuses_isComplete(SymRemoteNodeStatuses *this) {
    unsigned int complete = 0;
    SymIterator *iter = this->nodes->iterator(this->nodes);
    while (iter->hasNext(iter)) {
        SymRemoteNodeStatus *status = (SymRemoteNodeStatus *) iter->next(iter);
        complete |= status->complete;
    }
    return complete;
}

void SymRemoteNodeStatuses_destroy(SymRemoteNodeStatuses *this) {
    this->nodes->destroy(this->nodes);
    free(this);
}

SymRemoteNodeStatuses * SymRemoteNodeStatuses_new(SymRemoteNodeStatuses *this, SymMap *channels) {
    if (this == NULL) {
        this = (SymRemoteNodeStatuses *) calloc(1, sizeof(SymRemoteNodeStatuses));
    }
    this->channels = channels;
    this->nodes = SymList_new(NULL);
    this->wasDataProcessed = (void *) &SymRemoteNodeStatuses_wasDataProcessed;
    this->wasBatchProcessed = (void *) &SymRemoteNodeStatuses_wasBatchProcessed;
    this->getDataProcessedCount = (void *) &SymRemoteNodeStatuses_getDataProcessedCount;
    this->errorOccurred = (void *) &SymRemoteNodeStatuses_errorOccurred;
    this->add = (void *) &SymRemoteNodeStatuses_add;
    this->isComplete = (void *) &SymRemoteNodeStatuses_isComplete;
    this->destroy = (void *) &SymRemoteNodeStatuses_destroy;
    return this;
}
