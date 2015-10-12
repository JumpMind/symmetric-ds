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
#ifndef SYM_CHANNEL_H
#define SYM_CHANNEL_H

#include <stdlib.h>
#include "util/Date.h"

#define SYM_CHANNEL_DEFAULT_BATCH_ALGORITHM "default"
#define SYM_CHANNEL_DEFAULT_DATALOADER_TYPE "default"

typedef struct SymChannel {
    char *channelId;
    int processingOrder;
    int maxBatchSize;
    int maxBatchToSend;
    int maxDataToRoute;
    unsigned int enabled;
    unsigned int useOldDataToRoute;
    unsigned int useRowDataToRoute;
    unsigned int usePkDataToRoute;
    unsigned int containsBigLob;
    char *batchAlgorithm;
    long extractPeriodMillis;
    char *dataLoaderType;
    SymDate *createTime;
    SymDate *lastUpdateTime;
    char *lastUpdateBy;
    unsigned int reloadFlag;
    unsigned int fileSyncFlag;
    void (*destroy)(struct SymChannel *this);
} SymChannel;

SymChannel * SymChannel_new(SymChannel *this);

void SymChannel_destroy(SymChannel *this);

#endif
