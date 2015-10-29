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
#ifndef SYM_OUTGOING_BATCH_SERVICE_H
#define SYM_OUTGOING_BATCH_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include "model/OutgoingBatch.h"
#include "model/OutgoingBatches.h"
#include "db/platform/DatabasePlatform.h"
#include "service/ParameterService.h"
#include "service/SequenceService.h"
#include "util/List.h"
#include "util/StringBuilder.h"
#include "util/StringArray.h"
#include "util/AppUtils.h"
#include "common/Constants.h"

typedef struct SymOutgoingBatchService {
    SymDatabasePlatform *platform;
    SymParameterService *parameterService;
    SymSequenceService *sequenceService;
    SymOutgoingBatch * (*findOutgoingBatch)(struct SymOutgoingBatchService *this, long batchId, char *nodeId);
    SymOutgoingBatches * (*getOutgoingBatches)(struct SymOutgoingBatchService *this, char *nodeId);
    void (*insertOutgoingBatch)(struct SymOutgoingBatchService *this, SymOutgoingBatch *outgoingBatch);
    void (*updateOutgoingBatch)(struct SymOutgoingBatchService *this, SymOutgoingBatch *outgoingBatch);
    int (*countOutgoingBatchesUnsent)(struct SymOutgoingBatchService *this);
    int (*countOutgoingBatchesInError)(struct SymOutgoingBatchService *this);
    void (*destroy)(struct SymOutgoingBatchService *this);
} SymOutgoingBatchService;

SymOutgoingBatchService * SymOutgoingBatchService_new(SymOutgoingBatchService *this, SymDatabasePlatform *platform, SymParameterService *parameterService,
        SymSequenceService *sequenceService);

#define SYM_SQL_INSERT_OUTGOING_BATCH \
"insert into sym_outgoing_batch \
(batch_id, node_id, channel_id, status, load_id, extract_job_flag, load_flag, common_flag, reload_event_count, other_event_count, last_update_hostname, last_update_time, create_time, create_by) \
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp, current_timestamp, ?)"

#define SYM_SQL_UPDATE_OUTGOING_BATCH \
"update sym_outgoing_batch set status=?, load_id=?, extract_job_flag=?, load_flag=?, error_flag=?, \
byte_count=?, extract_count=?, sent_count=?, load_count=?, data_event_count=?, \
reload_event_count=?, insert_event_count=?, update_event_count=?, delete_event_count=?, other_event_count=?, \
ignore_count=?, router_millis=?, network_millis=?, filter_millis=?, \
load_millis=?, extract_millis=?, sql_state=?, sql_code=?, sql_message=?, \
failed_data_id=?, last_update_hostname=?, last_update_time=? where batch_id=? and node_id=?"

#define SYM_SQL_SELECT_OUTGOING_BATCH_PREFIX \
"select b.node_id, b.channel_id, b.status, \
b.byte_count, b.extract_count, b.sent_count, b.load_count, b.data_event_count, \
b.reload_event_count, b.insert_event_count, b.update_event_count, b.delete_event_count, b.other_event_count, \
b.ignore_count, b.router_millis, b.network_millis, b.filter_millis, b.load_millis, b.extract_millis, b.sql_state, b.sql_code, \
b.sql_message, \
b.failed_data_id, b.last_update_hostname, b.last_update_time, b.create_time, b.batch_id, b.extract_job_flag, b.load_flag, b.error_flag, b.common_flag, b.load_id, b.create_by \
from sym_outgoing_batch b "

#define SYM_SQL_FIND_OUTGOING_BATCH "where batch_id = ? and node_id = ?"
#define SYM_SQL_FIND_OUTGOING_BATCH_BY_ID_ONLY "where batch_id=? "

#define SYM_SQL_SELECT_OUTGOING_BATCH "where node_id = ? and status in (?, ?, ?, ?, ?, ?, ?) order by batch_id asc"

#define SYM_SQL_COUNT_OUTGOING_BATCHES_UNSENT "select count(*) from sym_outgoing_batch where status != 'OK'"

#define SYM_SQL_COUNT_OUTGOING_BATCHES_ERRORS "select count(*) from sym_outgoing_batch where error_flag=1"

#endif
