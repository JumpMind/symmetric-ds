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
#ifndef SYM_JOBMANAGER_H
#define SYM_JOBMANAGER_H

#include <stdlib.h>
#include "core/SymEngine.h"
#include "common/Log.h"

typedef struct SymJobManager {
    SymEngine *engine;
    unsigned short started;
    long lastPullTime;
    long lastPushTime;
    long lastHeartbeatTime;
    long lastPurgeTime;
    long lastRouteTime;
    long lastSyncTriggersTime;
    long lastOfflinePushTime;
    long lastOfflinePullTime;
    long lastFilePullTime;
    long lastFileTrackerTime;
    long lastFilePushTime;
    void (*startJobs)(struct SymJobManager *this);
    void (*stopJobs)(struct SymJobManager *this);
    void (*destroy)(struct SymJobManager *this);
} SymJobManager;

SymJobManager * SymJobManager_new(SymJobManager *this, SymEngine *engine);

#endif
