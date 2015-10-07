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
#include "model/NodeGroupLink.h"
#include "model/NodeSecurity.h"
#include "db/platform/DatabasePlatform.h"
#include "util/List.h"

typedef struct SymDataLoadStatus {
    int initialLoadEnabled;
    SymDate *initialLoadTime;
    void (*destroy)(struct SymDataLoadStatus *this);
} SymDataLoadStatus;

SymDataLoadStatus * SymDataLoadStatus_new();

typedef struct SymNodeService {
    SymDatabasePlatform *platform;

    SymNode * (*findIdentity)(struct SymNodeService *this);
    SymNodeSecurity * (*findNodeSecurity)(struct SymNodeService *this, char *nodeId);
    SymList * (*findNodesToPull)(struct SymNodeService *this);
    SymList * (*findNodesToPushTo)(struct SymNodeService *this);
    SymList * (*findSourceNodesFor)(struct SymNodeService *this, char *nodeGroupLinkAction);
    unsigned short (*isDataloadStarted)(struct SymNodeService *this);
    unsigned short (*isDataloadCompleted)(struct SymNodeService *this);
    int (*getNodeStatus)(struct SymNodeService *this);
    void (*destroy)(struct SymNodeService *);
} SymNodeService;

SymNodeService * SymNodeService_new(SymNodeService *this, SymDatabasePlatform *platform);

#define SYM_SQL_SELECT_NODE_PREFIX "select c.node_id, c.node_group_id, c.external_id, c.sync_enabled, \
c.sync_url, c.schema_version, c.database_type, \
c.database_version, c.symmetric_version, c.created_at_node_id, c.heartbeat_time, c.timezone_offset, \
c.batch_to_send_count, c.batch_in_error_count, c.deployment_type \
from sym_node c "

#define SYM_SQL_FIND_NODES_WHO_TARGET_ME \
"inner join sym_node_group_link d on \
c.node_group_id = d.source_node_group_id where d.target_node_group_id = ? and \
d.data_event_action = ? and c.node_id not in (select node_id from sym_node_identity)"

#define SYM_SQL_FIND_NODE_IDENTITY "inner join sym_node_identity i on c.node_id = i.node_id"

#define SYM_SQL_FIND_NODE_SECURITY \
"select node_id, node_password, registration_enabled, registration_time, \
initial_load_enabled, initial_load_time, created_at_node_id, \
rev_initial_load_enabled, rev_initial_load_time, initial_load_id, \
initial_load_create_by, rev_initial_load_id, rev_initial_load_create_by \
from sym_node_security where node_id = ?"

#define SYM_SQL_GET_DATALOAD_STATUS "select initial_load_enabled, initial_load_time from sym_node_security ns, \
sym_node_identity ni where ns.node_id=ni.node_id"

#endif
