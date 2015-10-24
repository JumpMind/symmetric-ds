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
#ifndef SYM_DATA_ROUTER_H
#define SYM_DATA_ROUTER_H

#include <stdlib.h>
#include "route/ChannelRouterContext.h"
#include "model/DataMetaData.h"
#include "model/TriggerRouter.h"
#include "model/OutgoingBatch.h"
#include "util/List.h"

typedef struct SymDataRouter {
    SymList * (*routeToNodes)(struct SymDataRouter *this, SymChannelRouterContext *context, SymDataMetaData *dataMetaData, SymList *nodes,
            unsigned short initialLoad, unsigned short initialLoadSelectUsed, SymTriggerRouter *triggerRouter);
    void (*completeBatch)(struct SymDataRouter *this, SymChannelRouterContext *context, SymOutgoingBatch *batch);
    void (*contextCommitted)(struct SymDataRouter *this, SymChannelRouterContext *context);
    unsigned short isConfigurable;
} SymDataRouter;

SymDataRouter * SymDataRouter_new(SymDataRouter *this);

#endif
