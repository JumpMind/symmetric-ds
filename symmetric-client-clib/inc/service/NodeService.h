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
#ifndef SYM_NODE_SERVICE_H
#define SYM_NODE_SERVICE_H

#include <stdio.h>
#include <stdlib.h>
#include "model/Node.h"
#include "model/NodeSecurity.h"
#include "db/platform/DatabasePlatform.h"
#include "util/List.h"

typedef struct SymNodeService {
    SymDatabasePlatform *platform;

    SymNode * (*findIdentity)(struct SymNodeService *this);
    SymNodeSecurity * (*findNodeSecurity)(struct SymNodeService *this, char *nodeId);
    SymList * (*findNodesToPull)(struct SymNodeService *this);
    SymList * (*findNodesToPushTo)(struct SymNodeService *this);
    unsigned short (*isDataloadStarted)(struct SymNodeService *this);
    void (*destroy)(struct SymNodeService *);
} SymNodeService;

SymNodeService * SymNodeService_new(SymNodeService *this, SymDatabasePlatform *platform);

#define SYM_SQL_SELECT_NODE_PREFIX "select c.node_id, c.node_group_id, c.external_id, c.sync_enabled, \
c.sync_url, c.schema_version, c.database_type, \
c.database_version, c.symmetric_version, c.created_at_node_id, c.heartbeat_time, c.timezone_offset, \
c.batch_to_send_count, c.batch_in_error_count, c.deployment_type \
from sym_node c "

#define SYM_SQL_FIND_NODE_IDENTITY "inner join sym_node_identity i on c.node_id = i.node_id"

#endif
