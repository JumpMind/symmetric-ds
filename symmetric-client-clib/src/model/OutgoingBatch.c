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
#include "model/OutgoingBatch.h"

long SymOutgoingBatch_totalEventCount(SymOutgoingBatch *this) {
    return this->insertEventCount + this->updateEventCount + this->deleteEventCount + this->otherEventCount;
}

void SymOutgoingBatch_destroy(SymOutgoingBatch *this) {
    free(this);
}

SymOutgoingBatch * SymOutgoingBatch_new(SymOutgoingBatch *this) {
    if (this == NULL) {
        this = (SymOutgoingBatch *) calloc(1, sizeof(SymOutgoingBatch));
    }
    this->batchId = -1;
    this->loadId = -1;
    this->status = SYM_OUGOING_BATCH_ROUTING;
    this->totalEventCount = &SymOutgoingBatch_totalEventCount;
    this->destroy = (void *) SymOutgoingBatch_destroy;
    return this;
}
