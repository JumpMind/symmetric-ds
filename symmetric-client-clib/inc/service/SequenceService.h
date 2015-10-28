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
#ifndef SYM_SEQUENCE_SERVICE_H
#define SYM_SEQUENCE_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include "db/platform/DatabasePlatform.h"
#include "db/sql/SqlTemplate.h"
#include "db/sql/SqlTransaction.h"
#include "model/Sequence.h"
#include "util/Map.h"
#include "util/List.h"
#include "util/StringArray.h"
#include "common/Constants.h"

typedef struct SymSequenceService {
    SymDatabasePlatform *platform;
    SymMap *sequenceDefinitionCache;
    void (*init)(struct SymSequenceService *this);
    long (*nextVal)(struct SymSequenceService *this, char *name);
    long (*currVal)(struct SymSequenceService *this, char *name);
    void (*destroy)(struct SymSequenceService *this);
} SymSequenceService;

SymSequenceService * SymSequenceService_new(SymSequenceService *this, SymDatabasePlatform *platform);

#define SYM_SQL_GET_SEQUENCE "select sequence_name,current_value,increment_by,min_value,max_value, \
cycle,create_time,last_update_by,last_update_time from sym_sequence where sequence_name=?"

#define SYM_SQL_GET_ALL_SEQUENCE "select sequence_name,current_value,increment_by,min_value,max_value, \
cycle,create_time,last_update_by,last_update_time from sym_sequence"

#define SYM_SQL_INSERT_SEQUENCE "insert into sym_sequence \
(sequence_name, current_value, increment_by, min_value, max_value, \
cycle, create_time, last_update_by, last_update_time) \
values(?,?,?,?,?,?,current_timestamp,?,current_timestamp)"

#define SYM_SQL_CURRENT_VALUE "select current_value from sym_sequence where sequence_name=?"

#define SYM_SQL_UPDATE_CURRENT_VALUE "update sym_sequence set current_value=?, last_update_time=current_timestamp \
where sequence_name=? and current_value=?"

#define SYM_SQL_MAX_OUTGOING_BATCH "select max(batch_id)+1 from sym_outgoing_batch"

#define SYM_SQL_MAX_TRIGGER_HIST "select max(trigger_hist_id)+1 from sym_trigger_hist"

#define SYM_SQL_MAX_EXTRACT_REQUEST "select max(request_id)+1 from sym_extract_request"

#endif
