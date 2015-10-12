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
#ifndef SYM_REMOTE_NODE_STATUSES_H
#define SYM_REMOTE_NODE_STATUSES_H

#include <stdlib.h>
#include <string.h>
#include "model/RemoteNodeStatus.h"
#include "util/List.h"
#include "util/Map.h"

typedef struct SymRemoteNodeStatuses {
    SymMap *channels;
    SymList *nodes;
    unsigned int (*wasDataProcessed)(struct SymRemoteNodeStatuses *this);
    unsigned int (*wasBatchProcessed)(struct SymRemoteNodeStatuses *this);
    long (*getDataProcessedCount)(struct SymRemoteNodeStatuses *this);
    unsigned int (*errorOccurred)(struct SymRemoteNodeStatuses *this);
    struct SymRemoteNodeStatus * (*add)(struct SymRemoteNodeStatuses *this, char *nodeId);
    unsigned int (*isComplete)(struct SymRemoteNodeStatuses *this);
    void (*destroy)(struct SymRemoteNodeStatuses *this);
} SymRemoteNodeStatuses;

SymRemoteNodeStatuses * SymRemoteNodeStatuses_new(SymRemoteNodeStatuses *this, SymMap *channels);

#endif
