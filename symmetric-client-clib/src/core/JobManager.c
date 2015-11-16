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
#include "core/JobManager.h"

unsigned short SymJobManager_shouldRun(SymJobManager *this, char* startJobProperty, char *periodTimeProperty, long lastTime) {

    if (! this->engine->parameterService->is(this->engine->parameterService, startJobProperty, 1)) {
        return 0;
    }

    long periodInMs =
            this->engine->parameterService->getInt(this->engine->parameterService, periodTimeProperty, 60000);
    long periodInSec = periodInMs / 1000;

    long now;
    time(&now);

    if (periodInSec < now-lastTime) {
        return 1;
    } else {
        return 0;
    }
}

void SymJobManager_invoke(SymJobManager *this) {
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_SYNCTRIGGERS_JOB, SYM_PARAMETER_SYNCTRIGGERS_PERIOD_MS, this->lastSyncTriggersTime)) {
        SymLog_info("SYNC TRIGGERS ============================)");
        this->engine->syncTriggers(this->engine);
        time(&this->lastSyncTriggersTime);
    }
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_ROUTE_JOB, SYM_PARAMETER_ROUTE_PERIOD_MS, this->lastRouteTime)) {
        SymLog_info("ROUTE ============================)");
        this->engine->route(this->engine);
        time(&this->lastRouteTime);
    }
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_PUSH_JOB, SYM_PARAMETER_PUSH_PERIOD_MS, this->lastPushTime)) {
        SymRemoteNodeStatuses *pushStatus = this->engine->push(this->engine);
        if (pushStatus->wasBatchProcessed(pushStatus)
                || pushStatus->wasDataProcessed(pushStatus)) {
            // Only run heartbeat after a successful push to avoid queueing up lots of offline heartbeats.
            if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_HEARTBEAT_JOB, SYM_PARAMETER_HEARTBEAT_JOB_PERIOD_MS, this->lastHeartbeatTime)) {
                SymLog_info("HEARTBEAT ============================)");
                this->engine->heartbeat(this->engine, 0);
                time(&this->lastHeartbeatTime);
            }
        }
        time(&this->lastPushTime);
        pushStatus->destroy(pushStatus);
    }
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_PULL_JOB, SYM_PARAMETER_PULL_PERIOD_MS, this->lastPullTime)) {
        SymLog_info("PULL ============================)");
        SymRemoteNodeStatuses *statuses = this->engine->pull(this->engine);
        time(&this->lastPullTime);
        statuses->destroy(statuses);
    }
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_PURGE_JOB, SYM_PARAMETER_PURGE_PERIOD_MS, this->lastPurgeTime)) {
        SymLog_info("PURGE ============================)");
        this->engine->purge(this->engine);
        time(&this->lastPurgeTime);
    }
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_OFFLINE_PUSH_JOB, SYM_PARAMETER_OFFLINE_PUSH_PERIOD_MS, this->lastOfflinePushTime)) {
        SymLog_info("OFFLINE PUSH ============================)");
        SymRemoteNodeStatuses *statuses = this->engine->offlinePushService->pushData(this->engine->offlinePushService);
        time(&this->lastOfflinePushTime);
        statuses->destroy(statuses);
    }
    if (SymJobManager_shouldRun(this, SYM_PARAMETER_START_OFFLINE_PULL_JOB, SYM_PARAMETER_OFFLINE_PULL_PERIOD_MS, this->lastOfflinePullTime)) {
        SymLog_info("OFFLINE PULL ============================)");
        SymRemoteNodeStatuses *statuses = this->engine->offlinePullService->pullData(this->engine->offlinePullService);
        time(&this->lastOfflinePullTime);
        statuses->destroy(statuses);
    }
}

void SymJobManager_startJobs(SymJobManager *this) {
    this->engine->start(this->engine);

    long now;
    time(&now);

    this->lastRouteTime = this->lastPullTime = this->lastPushTime =
            this->lastHeartbeatTime = this->lastPurgeTime = this->lastSyncTriggersTime =
                    this->lastOfflinePullTime = this->lastOfflinePushTime = now;

    this->started = 1;

    while (this->started) {
        long sleepPeriodInSec = this->engine->parameterService->getInt(this->engine->parameterService,
                SYM_PARAMETER_JOB_MANAGER_SLEEP_PERIOD_MS, 5000) / 1000;

        sleep(sleepPeriodInSec);
        SymJobManager_invoke(this);
    }
    this->engine->stop(this->engine);
}

void SymJobManager_stopJobs(SymJobManager *this) {
    this->started = 0;
}

void SymJobManager_destroy(SymJobManager *this) {
    free(this);
}

SymJobManager * SymJobManager_new(SymJobManager *this, SymEngine *engine) {
    if (this == NULL) {
        this = (SymJobManager *) calloc(1, sizeof(SymJobManager));
    }
    this->engine = engine;
    this->startJobs = (void *) &SymJobManager_startJobs;
    this->stopJobs = (void *) &SymJobManager_stopJobs;
    this->destroy = (void *) &SymJobManager_destroy;
    return this;
}
