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
#include "model/IncomingBatch.h"

void SymIncomingBatch_destroy(SymIncomingBatch *this) {
    free(this);
}

SymIncomingBatch * SymIncomingBatch_new(SymIncomingBatch *this) {
    if (this == NULL) {
        this = (SymIncomingBatch *) calloc(1, sizeof(SymIncomingBatch));
    }
    this->destroy = (void *) &SymIncomingBatch_destroy;
    return this;
}

SymIncomingBatch * SymIncomingBatch_newWithBatch(SymIncomingBatch *this, SymBatch *batch) {
    this = SymIncomingBatch_new(this);
    this->batchId = batch->batchId;
    this->nodeId = batch->sourceNodeId;
    this->channelId = batch->channelId;
    this->status = SYM_INCOMING_BATCH_STATUS_LOADING;
    return this;
}
