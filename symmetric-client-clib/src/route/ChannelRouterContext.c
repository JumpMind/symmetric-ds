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
#include "route/ChannelRouterContext.h"

void SymChannelRouterContext_addDataEvent(SymChannelRouterContext *this, long dataId, long batchId, char *routerId) {
    SymDataEvent *dataEvent = SymDataEvent_new(NULL, dataId, batchId, routerId);
    this->dataEventList->add(this->dataEventList, dataEvent);
}

void SymChannelRouterContext_commit(SymChannelRouterContext *this) {
    this->sqlTransaction->commit(this->sqlTransaction);
    this->clearState(this);
}

void SymChannelRouterContext_clearState(SymChannelRouterContext *this) {
    this->batchesByNodes->reset(this->batchesByNodes);
    this->availableNodes->reset(this->availableNodes);
    this->dataEventList->reset(this->dataEventList);
}

void SymChannelRouterContext_rollback(SymChannelRouterContext *this) {
    this->sqlTransaction->rollback(this->sqlTransaction);
    this->clearState(this);
}

void SymChannelRouterContext_cleanup(SymChannelRouterContext *this) {
    this->sqlTransaction->commit(this->sqlTransaction);
    this->sqlTransaction->close(this->sqlTransaction);
}

void SymChannelRouterContext_destroy(SymChannelRouterContext *this) {
    this->batchesByNodes->destroy(this->batchesByNodes);
    this->availableNodes->destroy(this->availableNodes);
    this->usedDataRouters->destroy(this->usedDataRouters);
    SymList_destroyAll(this->dataEventList, (void *) SymDataEvent_destroy);
    free(this);
}

SymChannelRouterContext * SymChannelRouterContext_new(SymChannelRouterContext *this, char *nodeId, SymChannel *channel, SymSqlTransaction *sqlTransaction) {
    if (this == NULL) {
        this = (SymChannelRouterContext *) calloc(1, sizeof(SymChannelRouterContext));
    }
    this->nodeId = nodeId;
    this->channel = channel;
    this->sqlTransaction = sqlTransaction;
    this->batchesByNodes = SymMap_new(NULL, 10);
    this->availableNodes = SymMap_new(NULL, 100);
    this->usedDataRouters = SymList_new(NULL);
    this->dataEventList = SymList_new(NULL);
    this->addDataEvent = &SymChannelRouterContext_addDataEvent;
    this->commit = &SymChannelRouterContext_commit;
    this->clearState = &SymChannelRouterContext_clearState;
    this->rollback = &SymChannelRouterContext_rollback;
    this->cleanup = &SymChannelRouterContext_cleanup;
    this->destroy = &SymChannelRouterContext_destroy;
    return this;
}
