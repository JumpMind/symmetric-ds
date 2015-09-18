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
#ifndef SYM_REMOTE_NODE_STATUS_H
#define SYM_REMOTE_NODE_STATUS_H

#include <stdlib.h>
#include <string.h>
#include "model/IncomingBatch.h"

#define SYM_REMOTE_NODE_STATUS_OFFLINE 0
#define SYM_REMOTE_NODE_STATUS_BUSY 1
#define SYM_REMOTE_NODE_STATUS_NOT_AUTHORIZED 2
#define SYM_REMOTE_NODE_STATUS_REGISTRATION_REQUIRED 3
#define SYM_REMOTE_NODE_STATUS_SYNC_DISABLED 4
#define SYM_REMOTE_NODE_STATUS_NO_DATA 5
#define SYM_REMOTE_NODE_STATUS_DATA_PROCESSED 6
#define SYM_REMOTE_NODE_STATUS_DATA_ERROR 7
#define SYM_REMOTE_NODE_STATUS_UNKNOWN_ERROR 8

typedef struct {
    char *nodeId;
    int status;
    long dataProcessed;
    long batchesProcessed;
    long reloadBatchesProcessed;
    int complete;
    int failed;
    void (*update_incoming_status)(void *this, SymIncomingBatch **incomingBatches);
    void (*destroy)(void *this);
} SymRemoteNodeStatus;

SymRemoteNodeStatus * SymRemoteNodeStatus_new(SymRemoteNodeStatus *this);

#endif
