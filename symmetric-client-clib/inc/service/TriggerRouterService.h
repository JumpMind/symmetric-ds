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
#include <time.h>
#include "db/model/Table.h"
#include "service/ParameterService.h"
#include "service/ConfigurationService.h"
#include "db/platform/DatabasePlatform.h"
#include "util/Map.h"
#include "util/List.h"
#include "model/Trigger.h"
#include "model/NodeGroupLink.h"
#include "model/TriggerHistory.h"
#include "model/TriggerRouter.h"
#include "model/Router.h"
#include "io/data/DataEventType.h"
#include "db/sql/SqlTemplate.h"
#include "common/ParameterConstants.h"
#include "common/Log.h"
#include "model/TriggerRouter.h"
#include "util/StringUtils.h"
#include "util/Map.h"
#include "service/SequenceService.h"
#include "model/Router.h"
#include "io/data/DataEventType.h"
#include "db/SymDialect.h"

typedef struct SymTriggerRouterService {
    SymConfigurationService *configurationService;
    SymSequenceService *sequenceService;
	SymParameterService *parameterService;
	SymDatabasePlatform *platform;
	SymDialect *symmetricDialect;
	SymMap *historyMap;
	SymMap *routersCache;
	time_t routersCacheTime;

	void (*syncTriggers)(struct SymTriggerRouterService *this, unsigned short force);
	SymTriggerHistory * (*getTriggerHistory)(struct SymTriggerRouterService *this, int histId);
	SymList * (*getActiveTriggerHistories)(struct SymTriggerRouterService *this);
	SymList * (*getActiveTriggerHistoriesByTrigger)(struct SymTriggerRouterService *this, SymTrigger *trigger);
	SymList * (*getActiveTriggerHistoriesByTableName)(struct SymTriggerRouterService *this, char *tableName);
	SymList * (*getRouters)(struct SymTriggerRouterService *this, unsigned short replaceVariables);
	SymRouter * (*getRouterById)(struct SymTriggerRouterService *this, char *routerId, unsigned short refreshCache);
    void (*destroy)(struct SymTriggerRouterService *this);
} SymTriggerRouterService;

SymTriggerRouterService * SymTriggerRouterService_new(SymTriggerRouterService *this,
        SymConfigurationService *configurationService, SymSequenceService *sequenceService,
        SymParameterService *parameterService, SymDatabasePlatform *platform, SymDialect *symmetricDialect);

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

#define SYM_SQL_SELECT_ROUTERS "select \
r.sync_on_insert as r_sync_on_insert,r.sync_on_update as r_sync_on_update,r.sync_on_delete as r_sync_on_delete,                            \
r.target_catalog_name,r.source_node_group_id,r.target_schema_name,r.target_table_name,r.target_node_group_id,r.router_expression,        \
r.router_type,r.router_id,r.create_time as r_create_time,r.last_update_time as r_last_update_time,r.last_update_by as r_last_update_by, \
r.use_source_catalog_schema \
from sym_router r order by r.router_id "

#define SYM_SQL_TRIGGER_HIST "select trigger_hist_id,trigger_id,source_table_name,table_hash,create_time,pk_column_names,column_names,\
last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,\
trigger_row_hash,trigger_template_hash,error_message \
from sym_trigger_hist where trigger_hist_id = ?"

#define SYM_SQL_ALL_TRIGGER_HIST "select trigger_hist_id,trigger_id,source_table_name,table_hash,create_time,pk_column_names,column_names,\
last_trigger_build_reason,name_for_delete_trigger,name_for_insert_trigger,name_for_update_trigger,source_schema_name,source_catalog_name,\
trigger_row_hash,trigger_template_hash,error_message \
from sym_trigger_hist "

#define SYM_SQL_ACTIVE_TRIGGER_HIST "where inactive_time is null"

#define SYM_SQL_TRIGGER_HIST_BY_SOURCE_TABLE_WHERE "where source_table_name = ? and inactive_time is null"

#define SYM_SQL_SELECT_ROUTERS_COLUMN_LIST \
"r.sync_on_insert as r_sync_on_insert,r.sync_on_update as r_sync_on_update,r.sync_on_delete as r_sync_on_delete, \
r.target_catalog_name,r.source_node_group_id,r.target_schema_name,r.target_table_name,r.target_node_group_id,r.router_expression, \
r.router_type,r.router_id,r.create_time as r_create_time,r.last_update_time as r_last_update_time,r.last_update_by as r_last_update_by, \
r.use_source_catalog_schema "

#define SYM_SQL_SELECT_ROUTERS "from sym_router r order by r.router_id"

#endif
