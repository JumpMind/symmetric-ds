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
#ifndef SYM_PARAMETER_SERVICE_H
#define SYM_PARAMETER_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "model/Node.h"
#include "util/Properties.h"

#define SYM_PARAMETER_ALL "ALL"

#define SYM_PARAMETER_DB_URL "db.url"
#define SYM_PARAMETER_DB_USER "db.user"
#define SYM_PARAMETER_DB_PASSWORD "db.password"

#define SYM_PARAMETER_GROUP_ID "group.id"
#define SYM_PARAMETER_EXTERNAL_ID "external.id"
#define SYM_PARAMETER_SYNC_URL "sync.url"
#define SYM_PARAMETER_REGISTRATION_URL "registration.url"
#define SYM_PARAMETER_SCHEMA_VERSION "schema.version"

#define SYM_PARAMETER_NODE_OFFLINE "node.offline"

#define SYM_PARAMETER_DATA_LOADER_NUM_OF_ACK_RETRIES "num.of.ack.retries"
#define SYM_PARAMETER_DATA_LOADER_TIME_BETWEEN_ACK_RETRIES "time.between.ack.retries.ms"

#define SYM_PARAMETER_REGISTRATION_NUMBER_OF_ATTEMPTS "registration.number.of.attempts"

#define SYM_PARAMETER_PARAMETER_REFRESH_PERIOD_IN_MS "parameter.reload.timeout.ms"

#define SYM_PARAMETER_INCOMING_BATCH_RECORD_OK_ENABLED "incoming.batches.record.ok.enabled"
#define SYM_PARAMETER_INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED "incoming.batches.skip.duplicates"

#define SYM_OUTGOING_BATCH_MAX_BATCHES_TO_SELECT "outgoing.batches.max.to.select"
#define SYM_TRANSPORT_MAX_BYTES_TO_SYNC "transport.max.bytes.to.sync"

typedef struct SymParameterService {
    SymProperties *parameters;
    SymProperties *properties;
    time_t lastTimeParameterWereCached;
    long cacheTimeoutInMs;
    char * (*getRegistrationUrl)(struct SymParameterService *this);
    char * (*getSyncUrl)(struct SymParameterService *this);
    char * (*getExternalId)(struct SymParameterService *this);
    char * (*getNodeGroupId)(struct SymParameterService *this);
    char * (*getString)(struct SymParameterService *this, char *name, char *defaultValue);
    long (*getLong)(struct SymParameterService *this, char *name, long defaultValue);
    int (*getInt)(struct SymParameterService *this, char *name, int defaultValue);
    unsigned short (*is)(struct SymParameterService *this, char *name, unsigned short defaultValue);
    void (*saveParameter)(struct SymParameterService *this, char *externalId, char *nodeGroupId, char *name, char *value, char *lastUpdatedBy);
    void (*deleteParameter)(struct SymParameterService *this, char *externalId, char *nodeGroupId, char *name);
    void (*destroy)(struct SymParameterService *this);
} SymParameterService;

SymParameterService * SymParameterService_new(SymParameterService * this, SymProperties *properties);

#endif
