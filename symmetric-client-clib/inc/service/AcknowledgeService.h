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
#ifndef SYM_ACKNOWLEDGE_SERVICE_H
#define SYM_ACKNOWLEDGE_SERVICE_H

#include <stdio.h>
#include "service/OutgoingBatchService.h"
#include "db/platform/DatabasePlatform.h"
#include "db/sql/mapper/StringMapper.h"
#include "model/BatchAck.h"
#include "model/BatchAckResult.h"
#include "util/List.h"
#include "util/StringArray.h"
#include "common/Log.h"

typedef struct SymAcknowledgeService {
    SymOutgoingBatchService *outgoingBatchService;
    SymDatabasePlatform *platform;
    SymBatchAckResult * (*ack)(struct SymAcknowledgeService *this, SymBatchAck *batchAck);
    void (*destroy)(struct SymAcknowledgeService *this);
} SymAcknowledgeService;

SymAcknowledgeService * SymAcknowledgeService_new(SymAcknowledgeService *this, SymOutgoingBatchService *outgoingBatchService, SymDatabasePlatform *platform);

#define SYM_SQL_SELECT_DATA_ID "select data_id from sym_data_event b where batch_id = ? order by data_id"

#endif
