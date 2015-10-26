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
#ifndef SYM_PURGE_SERVICE_H
#define SYM_PURGE_SERVICE_H

#include <stdio.h>
#include <time.h>
#include "common/Log.h"
#include "util/Date.h"
#include "util/List.h"
#include "service/ParameterService.h"
#include "db/SymDialect.h"
#include "util/StringArray.h"
#include "model/OutgoingBatch.h"

typedef enum {
    SYM_MIN_MAX_DELETE_DATA, SYM_MIN_MAX_DELETE_DATA_EVENT, SYM_MIN_MAX_DELETE_OUTGOING_BATCH, SYM_MIN_MAX_DELETE_STRANDED_DATA
} SymMinMaxDeleteSql;

typedef struct SymPurgeService {
    SymParameterService *parameterService;
    SymDialect *symmetricDialect;
    SymDatabasePlatform *platform;
    long (*purgeIncoming)(struct SymPurgeService *this);
    long (*purgeOutgoing)(struct SymPurgeService *this);
    long (*purgeIncomingBeforeDate)(struct SymPurgeService *this, SymDate *retentionCutoff);
    long (*purgeOutgoingBeforeDate)(struct SymPurgeService *this, SymDate *retentionCutoff);
    void (*destroy)(struct SymPurgeService *);
} SymPurgeService;

SymPurgeService * SymPurgeService_new(SymPurgeService *this, SymParameterService *parameterService,
        SymDialect *symmetricDialect, SymDatabasePlatform *platform);

#define SYM_SQL_UPDATE_STRANDED_BATCHES "\
update sym_outgoing_batch set status=? where node_id not \
in (select node_id from sym_node where sync_enabled=?) and status != ? "

#define SYM_SQL_SELECT_DATA_RANGE "\
select min(data_id) as min_id, max(data_id) as max_id from sym_data where data_id < (select max(data_id) from sym_data)"

#define SYM_SQL_DELETE_DATA "\
delete from sym_data where \
data_id between ? and ? and \
create_time < ? and \
data_id in (select e.data_id from sym_data_event e where \
e.data_id between ? and ?) \
and \
data_id not in \
(select e.data_id from sym_data_event e where \
e.data_id between ? and ? and \
(e.data_id is null or \
e.batch_id in \
(select batch_id from sym_outgoing_batch where \
status != ?))) "

#define SYM_SQL_DELETE_DATA_EVENT "\
delete from sym_data_event where batch_id not in (select batch_id from \
  sym_outgoing_batch where batch_id between ? and ? and status != ?) \
  and batch_id between ? and ? "

#define SYM_SQL_DELETE_OUTGOING_BATCH "\
delete from sym_outgoing_batch where status = ? and batch_id between ? \
  and ? and batch_id not in (select batch_id from sym_data_event where batch_id between ? \
  and ?) "

  /*and data_id < (select min(start_id) from $(data_gap)) and \ */

#define SYM_SQL_DELETE_STRANDED_DATA "\
delete from sym_data where \
  data_id between ? and ? and \
  create_time < ? and \
  data_id not in (select e.data_id from sym_data_event e where \
  e.data_id between ? and ?) "

#define SYM_SQL_SELECT_OUTGOING_BATCH_RANGE "\
select min(batch_id) as min_id, max(batch_id) as max_id from sym_outgoing_batch where \
  create_time < ? and status = ? and batch_id < (select max(batch_id) from sym_outgoing_batch) "

#define SYM_SQL_SELECT_INCOMING_BATCH_RANGE "\
select node_id, min(batch_id) as min_id, max(batch_id) as max_id from sym_incoming_batch where \
  create_time < ? and status = ? group by node_id "

#define SYM_SQL_PURGE_INCOMING_BATCH "\
delete from sym_incoming_batch where batch_id between ? and ? and node_id = ? and status = ? "

#endif
