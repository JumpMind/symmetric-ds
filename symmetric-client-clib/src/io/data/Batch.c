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
#include "io/data/Batch.h"

void SymBatch_destroy(SymBatch *this) {
    free(this->channelId);
    free(this->sourceNodeId);
    free(this->targetNodeId);
    free(this);
}

SymBatch * SymBatch_new(SymBatch *this) {
    if (this == NULL) {
        this = (SymBatch *) calloc(1, sizeof(SymBatch));
    }
    this->isIgnore = 0;
    this->destroy = (void *) &SymBatch_destroy;
    return this;
}

SymBatch * SymBatch_newWithSettings(SymBatch *this, long batchId, char *channelId, char *sourceNodeId, char *targetNodeId) {
    this = SymBatch_new(this);
    this->batchId = batchId;
    this->channelId = SymStringBuilder_copy(channelId);
    this->sourceNodeId = SymStringBuilder_copy(sourceNodeId);
    this->targetNodeId = SymStringBuilder_copy(targetNodeId);
    return this;
}
