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
#ifndef SYM_TABLE_CONSTANTS_H
#define SYM_TABLE_CONSTANTS_H

#include "util/List.h"

#define SYM_PARAMETER "sym_parameter"
#define SYM_LOCK "sym_lock"
#define SYM_OUTGOING_BATCH "sym_outgoing_batch"
#define SYM_EXTRACT_REQUEST "sym_extract_request"
#define SYM_INCOMING_BATCH "sym_incoming_batch"
#define SYM_TRIGGER "sym_trigger"
#define SYM_ROUTER "sym_router"
#define SYM_TRIGGER_HIST "sym_trigger_hist"
#define SYM_NODE_GROUP "sym_node_group"
#define SYM_NODE "sym_node"
#define SYM_NODE_HOST "sym_node_host"
#define SYM_DATA "sym_data"
#define SYM_DATA_GAP "sym_data_gap"
#define SYM_DATA_EVENT "sym_data_event"
#define SYM_TRANSFORM_TABLE "sym_transform_table"
#define SYM_LOAD_FILTER "sym_load_filter"
#define SYM_TRANSFORM_COLUMN "sym_transform_column"
#define SYM_TRIGGER_ROUTER "sym_trigger_router"
#define SYM_CHANNEL "sym_channel"
#define SYM_NODE_SECURITY "sym_node_security"
#define SYM_NODE_IDENTITY "sym_node_identity"
#define SYM_NODE_COMMUNICATION "sym_node_communication"
#define SYM_NODE_GROUP_LINK "sym_node_group_link"
#define SYM_NODE_HOST_STATS "sym_node_host_stats"
#define SYM_NODE_HOST_JOB_STATS "sym_node_host_job_stats"
#define SYM_REGISTRATION_REQUEST "sym_registration_request"
#define SYM_REGISTRATION_REDIRECT "sym_registration_redirect"
#define SYM_NODE_CHANNEL_CTL "sym_node_channel_ctl"
#define SYM_CONFLICT "sym_conflict"
#define SYM_NODE_GROUP_CHANNEL_WND "sym_node_group_channel_wnd"
#define SYM_NODE_HOST_CHANNEL_STATS "sym_node_host_channel_stats"
#define SYM_INCOMING_ERROR "sym_incoming_error"
#define SYM_SEQUENCE "sym_sequence"
#define SYM_TABLE_RELOAD_REQUEST "sym_table_reload_request"
#define SYM_GROUPLET "sym_grouplet"
#define SYM_GROUPLET_LINK "sym_grouplet_link"
#define SYM_TRIGGER_ROUTER_GROUPLET "sym_trigger_router_grouplet"
#define SYM_FILE_TRIGGER "sym_file_trigger"
#define SYM_FILE_TRIGGER_ROUTER "sym_file_trigger_router"
#define SYM_FILE_SNAPSHOT "sym_file_snapshot"
#define SYM_FILE_INCOMING "sym_file_incoming"
#define SYM_CONSOLE_USER "sym_console_user"
#define SYM_EXTENSION "sym_extension"

SymList * SymTableConstants_getConfigTables();

SymList * SymTableConstants_getTablesThatDoNotSync();

#endif
