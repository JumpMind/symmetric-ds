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
#ifndef SYM_CONFIGURATION_SERVICE_H
#define SYM_CONFIGURATION_SERVICE_H

#include <stdio.h>
#include "db/platform/DatabasePlatform.h"
#include "model/NodeChannel.h"
#include "model/Channel.h"
#include "util/List.h"
#include "util/Map.h"

typedef struct SymConfigurationService {
    SymDatabasePlatform *platform;
    SymList * (*getNodeChannels)(struct SymConfigurationService *this);
    SymMap * (*getChannels)(struct SymConfigurationService *this, unsigned int refreshCache);
    SymList * (*clearCache)(struct SymConfigurationService *this);
    SymList * (*destroy)(struct SymConfigurationService *this);
} SymConfigurationService;

SymConfigurationService * SymConfigurationService_new(SymConfigurationService *this, SymDatabasePlatform *platform);

#define SYM_SQL_SELECT_CHANNEL "select c.channel_id, c.processing_order, c.max_batch_size, c.enabled, \
c.max_batch_to_send, c.max_data_to_route, c.use_old_data_to_route, \
c.use_row_data_to_route, c.use_pk_data_to_route, c.contains_big_lob, \
c.batch_algorithm, c.extract_period_millis, c.data_loader_type, \
c.last_update_time, c.last_update_by, c.create_time, c.reload_flag, c.file_sync_flag \
from sym_channel c order by c.processing_order asc, c.channel_id"

#endif
