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
#ifndef SYM_INCOMING_BATCH_SERVICE_H
#define SYM_INCOMING_BATCH_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "model/IncomingBatch.h"
#include "db/platform/DatabasePlatform.h"
#include "service/ParameterService.h"
#include "util/StringBuilder.h"
#include "util/StringArray.h"
#include "util/AppUtils.h"
#include "common/ParameterConstants.h"

typedef struct SymIncomingBatchService {
    SymDatabasePlatform *platform;
    SymParameterService *parameterService;
    SymIncomingBatch * (*findIncomingBatch)(struct SymIncomingBatchService *this, long batchId, char *nodeId);
    unsigned short (*acquireIncomingBatch)(struct SymIncomingBatchService *this, SymIncomingBatch *incomingBatch);
    int (*insertIncomingBatch)(struct SymIncomingBatchService *this, SymIncomingBatch *incomingBatch);
    int (*updateIncomingBatch)(struct SymIncomingBatchService *this, SymIncomingBatch *incomingBatch);
    int (*deleteIncomingBatch)(struct SymIncomingBatchService *this, SymIncomingBatch *incomingBatch);
    unsigned short (*isRecordOkBatchesEnabled)(struct SymIncomingBatchService *this);
    int (*countIncomingBatchesInError)(struct SymIncomingBatchService *this);
    void (*destroy)(struct SymIncomingBatchService *this);
} SymIncomingBatchService;

SymIncomingBatchService * SymIncomingBatchService_new(SymIncomingBatchService *this, SymDatabasePlatform *platform, SymParameterService *parameterService);

#define SYM_SQL_SELECT_INCOMING_BATCH_PREFIX \
"select batch_id, node_id, channel_id, status, network_millis, filter_millis, database_millis, failed_row_number, failed_line_number, byte_count, \
statement_count, fallback_insert_count, fallback_update_count, ignore_count, missing_delete_count, skip_count, sql_state, sql_code, sql_message, \
last_update_hostname, last_update_time, create_time, error_flag from sym_incoming_batch "

#define SYM_SQL_FIND_INCOMING_BATCH "where batch_id = ? and node_id = ?"

#define SYM_SQL_INSERT_INCOMING_BATCH \
"insert into sym_incoming_batch \
(batch_id, node_id, channel_id, status, network_millis, filter_millis, database_millis, failed_row_number, failed_line_number, byte_count, \
statement_count, fallback_insert_count, fallback_update_count, ignore_count, missing_delete_count, skip_count, sql_state, sql_code, sql_message, \
last_update_hostname, last_update_time, create_time) \
values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, current_timestamp)"

#define SYM_SQL_UPDATE_INCOMING_BATCH \
"update sym_incoming_batch \
set status = ?, error_flag=?, network_millis = ?, filter_millis = ?, database_millis = ?, failed_row_number = ?, \
failed_line_number = ?, byte_count = ?, statement_count = ?, fallback_insert_count = ?, fallback_update_count = ?, ignore_count = ?, \
missing_delete_count = ?, skip_count = ?,  sql_state = ?, sql_code = ?, sql_message = ?, last_update_hostname = ?, last_update_time = ? \
where batch_id = ? and node_id = ?"

#define SYM_SQL_DELETE_INCOMING_BATCH "delete from sym_incoming_batch where batch_id = ? and node_id = ?"

#define SYM_SQL_COUNT_INCOMING_BATCHES_ERRORS "select count(*) from sym_incoming_batch where error_flag = 1"

#endif
