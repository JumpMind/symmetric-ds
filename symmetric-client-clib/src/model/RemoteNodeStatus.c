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

void SymRemoteNodeStatus_update_incoming_status(SymRemoteNodeStatus *this, SymIncomingBatch **incomingBatches) {
    if (incomingBatches != NULL) {
        int i = 0;
        for (; incomingBatches[i] != NULL; i++) {
            SymIncomingBatch *incomingBatch = incomingBatches[i];
            this->dataProcessed += incomingBatch->statementCount;
            this->batchesProcessed++;
            if (strcmp(incomingBatch->status, SYM_INCOMING_BATCH_STATUS_ERROR) == 0) {
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

SymRemoteNodeStatus * SymRemoteNodeStatus_new(SymRemoteNodeStatus *this) {
    if (this == NULL) {
        this = (SymRemoteNodeStatus *) calloc(1, sizeof(SymRemoteNodeStatus));
    }
    this->destroy = (void *) &SymRemoteNodeStatus_destroy;
    this->update_incoming_status = (void *) &SymRemoteNodeStatus_update_incoming_status;
    return this;
}
