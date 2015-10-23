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
#include "route/DefaultDataRouter.h"

SymList * SymDefaultDataRouter_routeToNodes(SymDefaultDataRouter *this, SymChannelRouterContext *context, SymDataMetaData *dataMetaData, SymList *nodes,
        unsigned short initialLoad, unsigned short initialLoadSelectUsed, SymTriggerRouter *triggerRouter) {
    SymList *nodeIds = SymList_new(NULL);
    SymIterator *iter = nodes->iterator(nodes);
    while (iter->hasNext(iter)) {
        SymNode *node = (SymNode *) iter->next(iter);
        nodeIds->add(nodeIds, node->nodeId);
    }
    iter->destroy(iter);
    return nodeIds;
}

void SymDefaultDataRouter_completeBatch(SymDefaultDataRouter *this, SymChannelRouterContext *context, SymOutgoingBatch *batch) {
}

void SymDefaultDataRouter_contextCommitted(SymDefaultDataRouter *this, SymChannelRouterContext *context) {
}

void SymDefaultDataRouter_destroy(SymDefaultDataRouter *this) {
    free(this);
}

SymDefaultDataRouter * SymDefaultDataRouter_new(SymDefaultDataRouter *this) {
    if (this == NULL) {
        this = (SymDefaultDataRouter *) calloc(1, sizeof(SymDefaultDataRouter));
    }
    SymDataRouter *super = &this->super;
    super->routeToNodes = (void *) &SymDefaultDataRouter_routeToNodes;
    super->completeBatch = (void *) &SymDefaultDataRouter_completeBatch;
    super->contextCommitted = (void *) &SymDefaultDataRouter_contextCommitted;
    this->destroy = (void *) &SymDefaultDataRouter_destroy;
    return this;
}
