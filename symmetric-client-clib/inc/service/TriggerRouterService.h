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
#ifndef SYM_TRIGGER_ROUTER_SERVICE_H
#define SYM_TRIGGER_ROUTER_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include "db/model/Table.h"
#include "service/ParameterService.h"
#include "service/ConfigurationService.h"
#include "db/platform/DatabasePlatform.h"
#include "util/Map.h"
#include "util/List.h"
#include "model/Trigger.h"
#include "model/TriggerHistory.h"
#include "model/NodeGroupLink.h"
#include "model/TriggerRouter.h"
#include "model/Router.h"
#include "io/data/DataEventType.h"
#include "db/sql/SqlTemplate.h"
#include "common/ParameterConstants.h"
#include "common/Log.h"
#include "model/TriggerRouter.h"
#include <time.h>
#include "util/StringUtils.h"
#include "service/SequenceService.h"

typedef struct SymTriggerRouterService {
    SymConfigurationService *configurationService;
    SymSequenceService *sequenceService;
	SymParameterService *parameterService;
	SymDatabasePlatform *platform;

	void (*syncTriggers)(struct SymTriggerRouterService *this, unsigned short force);
    void (*destroy)(struct SymTriggerRouterService *this);
} SymTriggerRouterService;

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService *this,
        SymConfigurationService *configurationService, SymSequenceService *sequenceService,
        SymParameterService *parameterService, SymDatabasePlatform *platform);

#define SYM_SQL_SELECT_TRIGGER_ROUTERS "select tr.trigger_id, tr.router_id, tr.create_time, tr.last_update_time, tr.last_update_by, tr.initial_load_order, tr.initial_load_select, tr.initial_load_delete_stmt, tr.initial_load_batch_count, tr.ping_back_enabled, tr.enabled \
from sym_trigger_router tr \
inner join sym_trigger t on tr.trigger_id=t.trigger_id \
inner join sym_router r on tr.router_id=r.router_id      "

#define SYM_SQL_SELECT_TRIGGERS "select t.trigger_id,t.channel_id,t.reload_channel_id,t.source_table_name,t.source_schema_name,t.source_catalog_name,        \
t.sync_on_insert,t.sync_on_update,t.sync_on_delete,t.sync_on_incoming_batch,t.use_stream_lobs,   \
t.use_capture_lobs,t.use_capture_old_data,t.use_handle_key_updates,                              \
t.excluded_column_names, t.sync_key_names,                                                       \
t.name_for_delete_trigger,t.name_for_insert_trigger,t.name_for_update_trigger,                   \
t.sync_on_insert_condition,t.sync_on_update_condition,t.sync_on_delete_condition,                \
t.custom_on_insert_text,t.custom_on_update_text,t.custom_on_delete_text,                          \
t.tx_id_expression,t.external_select,t.channel_expression,t.create_time as t_create_time,         \
t.last_update_time as t_last_update_time, t.last_update_by as t_last_update_by \
from sym_trigger t order by trigger_id asc   "

#endif
