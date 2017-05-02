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
#include "model/NodeGroupLinkAction.h"
#include "model/NodeSecurity.h"
#include "db/platform/DatabasePlatform.h"
#include "util/List.h"
#include "model/NodeHost.h"

typedef struct SymDataLoadStatus {
    int initialLoadEnabled;
    SymDate *initialLoadTime;
    void (*destroy)(struct SymDataLoadStatus *this);
} SymDataLoadStatus;

SymDataLoadStatus * SymDataLoadStatus_new();

typedef struct SymNodeService {
    SymDatabasePlatform *platform;
    SymNode *cachedNodeIdentity;
    SymDate *lastRestartTime;

    SymNode * (*findIdentity)(struct SymNodeService *this);
    SymNode * (*findIdentityWithCache)(struct SymNodeService *this, unsigned short useCache);
    SymNodeSecurity * (*findNodeSecurity)(struct SymNodeService *this, char *nodeId);
    SymList * (*findNodesToPull)(struct SymNodeService *this);
    SymList * (*findNodesToPushTo)(struct SymNodeService *this);
    SymList * (*findSourceNodesFor)(struct SymNodeService *this, SymNodeGroupLinkAction nodeGroupLinkAction);
    SymList * (*findTargetNodesFor)(struct SymNodeService *this, SymNodeGroupLinkAction nodeGroupLinkAction);
    SymNode * (*findNode)(struct SymNodeService *this, char* nodeId);
    unsigned short (*isDataloadStarted)(struct SymNodeService *this);
    unsigned short (*isDataloadCompleted)(struct SymNodeService *this);
    int (*getNodeStatus)(struct SymNodeService *this);
    void (*save)(struct SymNodeService *this, SymNode *node);
    void (*updateNodeHostForCurrentNode)(struct SymNodeService *this);
    SymList * (*findEnabledNodesFromNodeGroup)(struct SymNodeService *this, char *nodeGroupId);
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

#define SYM_SQL_FIND_NODES_WHO_I_TARGET \
"inner join sym_node_group_link d on \
c.node_group_id = d.target_node_group_id where d.source_node_group_id = ? and \
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

#define SYM_SQL_INSERT_NODE "\
insert into sym_node (node_group_id, external_id, database_type, database_version, schema_version, symmetric_version, sync_url, \
heartbeat_time, sync_enabled, timezone_offset, batch_to_send_count, batch_in_error_count, created_at_node_id, \
deployment_type, node_id) values (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)"

#define SYM_SQL_UPDATE_NODE "\
update sym_node set node_group_id=?, external_id=?, database_type=?, \
database_version=?, schema_version=?, symmetric_version=?, sync_url=?, heartbeat_time=?, \
sync_enabled=?, timezone_offset=?, batch_to_send_count=?, batch_in_error_count=?, created_at_node_id=?, deployment_type=? where node_id = ? "

#define SYM_SQL_INSERT_NODE_HOST "\
insert into sym_node_host \
(ip_address, os_user, os_name, os_arch, os_version, available_processors, free_memory_bytes, total_memory_bytes, max_memory_bytes, java_version, java_vendor, jdbc_version, symmetric_version, timezone_offset, heartbeat_time, last_restart_time, create_time, node_id, host_name) \
values(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?, current_timestamp,?,?) "

#define SYM_SQL_UPDATE_NODE_HOST "\
update sym_node_host set \
ip_address=?, os_user=?, os_name=?, os_arch=?, os_version=?, available_processors=?, free_memory_bytes=?, \
total_memory_bytes=?, max_memory_bytes=?, java_version=?, java_vendor=?, jdbc_version=?, symmetric_version=?, timezone_offset=?, heartbeat_time=?, \
last_restart_time=? where node_id=? and host_name=? "

#define SYM_SQL_FIND_ENABLED_NODES_FROM_NODE_GROUP_SQL "where node_group_id = ? and sync_enabled=1 order by node_id"

#define SYM_SQL_FIND_NODE "where node_id = ?   "

#endif
