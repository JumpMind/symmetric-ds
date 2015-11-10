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
#include "model/Channel.h"

void SymChannel_destroy(SymChannel *this) {
    if (this->createTime) {
        this->createTime->destroy(this->createTime);
    }
    if (this->lastUpdateTime) {
        this->lastUpdateTime->destroy(this->lastUpdateTime);
    }
    free(this);
}

SymChannel * SymChannel_new(SymChannel *this) {
    if (this == NULL) {
        this = (SymChannel *) calloc(1, sizeof(SymChannel));
    }
    this->maxBatchSize = 10000;
    this->maxBatchToSend = 100;
    this->maxDataToRoute = 10000;
    this->enabled = 1;
    this->useOldDataToRoute = 1;
    this->useRowDataToRoute = 1;
    this->usePkDataToRoute = 1;
    this->batchAlgorithm = SYM_CHANNEL_DEFAULT_BATCH_ALGORITHM;
    this->dataLoaderType = SYM_CHANNEL_DEFAULT_DATALOADER_TYPE;
    this->destroy = (void *) SymChannel_destroy;
    return this;
}
