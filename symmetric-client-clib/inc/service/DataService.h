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
#ifndef SYM_DATA_SERVICE_H
#define SYM_DATA_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "db/SymDialect.h"
#include "db/platform/DatabasePlatform.h"
#include "service/TriggerRouterService.h"
#include "service/OutgoingBatchService.h"
#include "service/NodeService.h"
#include "service/ParameterService.h"
#include "model/Data.h"
#include "model/DataEvent.h"
#include "model/TriggerHistory.h"
#include "io/data/Batch.h"
#include "io/data/DataEventType.h"
#include "util/List.h"
#include "util/StringBuilder.h"
#include "util/StringUtils.h"
#include "common/Log.h"
#include "common/ParameterConstants.h"

typedef struct SymDataService {
    SymDatabasePlatform *platform;
    SymTriggerRouterService *triggerRouterService;
    SymNodeService *nodeService;
    SymDialect *dialect;
    SymOutgoingBatchService *outgoingBatchService;
    SymParameterService *parameterService;

    void (*heartbeat)(struct SymDataService *this, unsigned short force);
    SymList * (*selectDataFor)(struct SymDataService *this, SymBatch *batch);
    void (*insertDataEvents)(struct SymDataService *this, SymSqlTransaction *transaction, SymList *events);
    SymData * (*mapData)(struct SymDataService *this, SymRow *row);
    void (*destroy)(struct SymDataService *this);
} SymDataService;

SymDataService * SymDataService_new(SymDataService *this, SymDatabasePlatform *platform, SymTriggerRouterService *triggerRouterService,
        SymNodeService *nodeService, SymDialect *dialect, SymOutgoingBatchService *outgoingBatchService,
        SymParameterService *parameterService);

#define SYM_SQL_SELECT_EVENT_DATA_TO_EXTRACT \
"select d.data_id, d.table_name, d.event_type, d.row_data as row_data, d.pk_data as pk_data, d.old_data as old_data, \
d.create_time, d.trigger_hist_id, d.channel_id, d.transaction_id, d.source_node_id, d.external_data, d.node_list, e.router_id \
from sym_data d \
inner join sym_data_event e on d.data_id = e.data_id \
inner join sym_outgoing_batch o on o.batch_id = e.batch_id \
where o.batch_id = ? and o.node_id = ?"

#define SYM_SQL_INSERT_INTO_DATA_EVENT "insert into sym_data_event (data_id, batch_id, router_id, create_time) values(?, ?, ?, current_timestamp)"

#endif
