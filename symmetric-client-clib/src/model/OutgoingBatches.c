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
#include "model/OutgoingBatches.h"

void SymOutgoingBatches_destroy(SymOutgoingBatches *this) {
    this->batches->destroy(this->batches);
    free(this);
}

SymOutgoingBatches * SymOutgoingBatches_new(SymOutgoingBatches *this) {
    if (this == NULL) {
        this = (SymOutgoingBatches *) calloc(1, sizeof(SymOutgoingBatches));
    }
    this->batches = SymList_new(NULL);
    this->destroy = (void *) SymOutgoingBatches_destroy;
    return this;
}

SymOutgoingBatches * SymOutgoingBatches_newWithList(SymOutgoingBatches *this, SymList *list) {
    this = SymOutgoingBatches_new(this);
    this->batches->addAll(this->batches, list);
    return this;
}
