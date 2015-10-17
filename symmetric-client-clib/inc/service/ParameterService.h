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
#include "common/ParameterConstants.h"

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
