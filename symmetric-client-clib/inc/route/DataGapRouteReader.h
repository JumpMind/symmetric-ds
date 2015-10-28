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
#ifndef SYM_DATA_GAP_ROUTE_READER_H
#define SYM_DATA_GAP_ROUTE_READER_H

#include <stdlib.h>
#include "service/ParameterService.h"
#include "service/DataService.h"
#include "db/platform/DatabasePlatform.h"
#include "route/ChannelRouterContext.h"
#include "util/List.h"
#include "util/StringBuilder.h"

typedef struct SymDataGapRouteReader {
    SymDatabasePlatform *platform;
    SymParameterService *parameterService;
    SymDataService *dataService;
    SymChannelRouterContext *context;
    SymList * (*selectDataFor)(struct SymDataGapRouteReader *this);
    void (*destroy)(struct SymDataGapRouteReader *);
} SymDataGapRouteReader;

SymDataGapRouteReader * SymDataGapRouteReader_new(SymDataGapRouteReader *this, SymDatabasePlatform *platform, SymParameterService *parameterService,
        SymDataService *dataService, SymChannelRouterContext *context);

#define SYM_SQL_SELECT_DATA_USING_CHANNEL_ID \
"select d.data_id, d.table_name, d.event_type, d.row_data as row_data, d.pk_data as pk_data, d.old_data as old_data, \
d.create_time, d.trigger_hist_id, d.channel_id, d.transaction_id, d.source_node_id, d.external_data, d.node_list \
from sym_data d left join sym_data_event e on e.data_id = d.data_id where e.data_id is null and d.channel_id = ? "

#define SYM_SQL_ORDER_BY_DATA_ID " order by d.data_id asc "

#endif
