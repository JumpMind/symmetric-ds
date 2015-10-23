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
#ifndef SYM_OUTGOING_BATCH_H
#define SYM_OUTGOING_BATCH_H

#include <stdio.h>
#include <stdlib.h>
#include "util/Date.h"
#include "io/data/DataEventType.h"

#define SYM_OUTGOING_BATCH_OK "OK"
#define SYM_OUTGOING_BATCH_ERROR "ER"
#define SYM_OUTGOING_BATCH_REQUEST "RQ"
#define SYM_OUTGOING_BATCH_NEW "NE"
#define SYM_OUTGOING_BATCH_QUERYING "QY"
#define SYM_OUTGOING_BATCH_SENDING "SE"
#define SYM_OUTGOING_BATCH_LOADING "LD"
#define SYM_OUTGOING_BATCH_ROUTING "RT"
#define SYM_OUTGOING_BATCH_IGNORED "IG"

typedef struct SymOutgoingBatch {
    long batchId;
    char *nodeId;
    char *channelId;
    long loadId;
    char *status;
    unsigned short loadFlag;
    unsigned short errorFlag;
    unsigned short extractJobFlag;
    unsigned short commonFlag;
    long routerMillis;
    long networkMillis;
    long filterMillis;
    long loadMillis;
    long extractMillis;
    long byteCount;
    long sentCount;
    long extractCount;
    long loadCount;
    long ignoreCount;
    long dataEventCount;
    long reloadEventCount;
    long insertEventCount;
    long updateEventCount;
    long deleteEventCount;
    long otherEventCount;
    long failedDataId;
    char *sqlState;
    int sqlCode;
    char *sqlMessage;
    char *lastUpdatedHostName;
    SymDate *lastUpdatedTime;
    SymDate *createTime;
    char *createBy;
    long (*totalEventCount)(struct SymOutgoingBatch *this);
    void (*incrementEventCount)(struct SymOutgoingBatch *this, SymDataEventType type);
    void (*destroy)(struct SymOutgoingBatch *this);
} SymOutgoingBatch;

SymOutgoingBatch * SymOutgoingBatch_new(SymOutgoingBatch *this);

SymOutgoingBatch * SymOutgoingBatch_newWithNode(SymOutgoingBatch *this, char *nodeId, char *channelId, char *status);

#endif
