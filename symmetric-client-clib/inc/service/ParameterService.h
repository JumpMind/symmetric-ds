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
#include "db/platform/DatabasePlatform.h"
#include "db/sql/SqlTemplate.h"
#include "model/Node.h"
#include "model/DatabaseParameter.h"
#include "util/Properties.h"
#include "util/StringArray.h"
#include "common/ParameterConstants.h"

typedef struct SymParameterService {
    SymProperties *parameters;
    SymProperties *properties;
    struct SymDatabasePlatform *platform;
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
    SymList * (*getDatabaseParametersFor)(struct SymParameterService *this, char *paramKey);
    void (*destroy)(struct SymParameterService *this);
} SymParameterService;

SymParameterService * SymParameterService_new(SymParameterService * this, SymProperties *properties, struct SymDatabasePlatform *platform);

#define SYM_SQL_SELECT_PARAMETERS "select param_key, param_value from sym_parameter where external_id=? and node_group_id=?"

#define SYM_SQL_UPDATE_PARAMETER "update sym_parameter set param_value=?, last_update_by=?, last_update_time=current_timestamp \
where external_id=? and node_group_id=? and param_key=?"

#define SYM_SQL_INSERT_PARAMETER "insert into sym_parameter \
(external_id, node_group_id, param_key, param_value, last_update_by, create_time, last_update_time) \
values(?, ?, ?, ?, ?, current_timestamp, current_timestamp)"

#define SYM_SQL_DELETE_PARAMETER "delete from sym_parameter where external_id=? and node_group_id=? and param_key=?"

#define SYM_SELECT_PARAMETERS_BY_KEY "select param_key, param_value, external_id, node_group_id from sym_parameter where param_key=? \
order by node_group_id, external_id"

#endif
