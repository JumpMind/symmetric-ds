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
#ifndef SYM_CHANNEL_ROUTER_CONTEXT_H
#define SYM_CHANNEL_ROUTER_CONTEXT_H

#include <stdlib.h>
#include "model/Channel.h"
#include "model/DataEvent.h"
#include "db/sql/SqlTransaction.h"
#include "util/Map.h"
#include "util/List.h"

typedef struct SymChannelRouterContext {
    SymChannel *channel;
    char *nodeId;
    SymMap *batchesByNodes;
    SymMap *availableNodes;
    SymList *usedDataRouters;
    unsigned short needsCommitted;
    SymList *dataEventList;
    SymSqlTransaction *sqlTransaction;
    long statInsertDataEventsMs;
    long statDataRouterMs;
    long statDataRoutedCount;
    long statDataEventsInserted;
    void (*addDataEvent)(struct SymChannelRouterContext *this, long dataId, long batchId, char *routerId);
    void (*commit)(struct SymChannelRouterContext *this);
    void (*rollback)(struct SymChannelRouterContext *this);
    void (*clearState)(struct SymChannelRouterContext *this);
    void (*cleanup)(struct SymChannelRouterContext *this);
    void (*destroy)(struct SymChannelRouterContext *this);
} SymChannelRouterContext;

SymChannelRouterContext * SymChannelRouterContext_new(SymChannelRouterContext *this, char *nodeId, SymChannel *channel, SymSqlTransaction *sqlTransaction);

#endif
