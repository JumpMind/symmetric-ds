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
#include "model/DataEvent.h"

void SymDataEvent_destroy(SymDataEvent *this) {
    free(this);
}

SymDataEvent * SymDataEvent_new(SymDataEvent *this, long dataId, long batchId, char *routerId) {
    if (this == NULL) {
        this = (SymDataEvent *) calloc(1, sizeof(SymDataEvent));
    }
    this->dataId = dataId;
    this->batchId = batchId;
    this->routerId = routerId;
    this->destroy = (void *) SymDataEvent_destroy;
    return this;
}
