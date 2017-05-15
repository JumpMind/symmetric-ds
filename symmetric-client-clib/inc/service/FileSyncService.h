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
#ifndef SYM_FILESYNCSERVICE_H_
#define SYM_FILESYNCSERVICE_H_

#include <stdlib.h>
#include "model/FileTrigger.h"
#include "model/FileTriggerRouter.h"
#include "file/DirectorySnapshot.h"
#include "model/FileSnapshot.h"
#include "file/FileTriggerTracker.h"
#include "common/ParameterConstants.h"
#include "db/sql/SqlTransaction.h"
#include "model/NodeSecurity.h"
#include "model/RemoteNodeStatus.h"
#include "model/Node.h"
#include "transport/OutgoingTransport.h"
#include "model/OutgoingBatches.h"
#include "file/FileSyncZipDataWriter.h"
#include "service/DataLoaderService.h"
#include "util/StringUtils.h"

struct SymEngine;

typedef struct SymFileSyncService {
    struct SymEngine *engine;
    void (*trackChanges)(struct SymFileSyncService *this, unsigned short force);
    SymList * (*getFileTriggerRoutersForCurrentNode)(struct SymFileSyncService *this);
    SymFileTrigger * (*getFileTrigger)(struct SymFileSyncService *this, char* triggerId);
    SymDirectorySnapshot * (*getDirectorySnapshot)(struct SymFileSyncService *this, SymFileTriggerRouter *fileTriggerRouter);
    long (*saveDirectorySnapshot)(struct SymFileSyncService *this, SymFileTriggerRouter *fileTriggerRouter, SymDirectorySnapshot *dirSnapshot);
    void (*save)(struct SymFileSyncService *this, SymDirectorySnapshot *dirSnapshot);
    int (*saveSnapshot)(struct SymFileSyncService *this, SymSqlTransaction *transaction, SymFileSnapshot *snapshot);
    char * (*findSourceNodeIdFromFileIncoming)(struct SymFileSyncService *this, char *filePath, char *fileName, long lastUpdateDate);
    SymRemoteNodeStatuses * (*pushFilesToNodes)(struct SymFileSyncService *this);
    void (*pushFilesToNode)(struct SymFileSyncService *this, SymNode *remote, SymRemoteNodeStatus *status);
    SymList * /*<OutgoingBatch>*/ (*sendFiles)(struct SymFileSyncService *this, SymNode *sourceNode, SymNode *targetNode, SymOutgoingTransport *transport);
    SymList * /*<OutgoingBatch>*/ (*getBatchesToProcess)(struct SymFileSyncService *this, SymNode *targetNode);
    SymFileTriggerRouter * (*getFileTriggerRouter)(struct SymFileSyncService *this, char* triggerId, char* routerId);
    SymRemoteNodeStatuses * (*pullFilesFromNodes)(struct SymFileSyncService *this);
    void  (*pullFilesFromNode)(struct SymFileSyncService *this, SymNode *remote, SymRemoteNodeStatus *status);
    SymList * /*<IncomingBatch>*/ (*processZip)(struct SymFileSyncService *this, SymIncomingTransport *transport, char* sourceNodeId, SymNode* identity);
    void (*updateFileIncoming)(struct SymFileSyncService *this, char* nodeId, SymProperties* filesToEventType);
    void (*cleanExtractDir)(struct SymFileSyncService *this);
    void (*destroy)(struct SymFileSyncService *this);
} SymFileSyncService;

SymFileSyncService * SymFileSyncService_new(SymFileSyncService *this, struct SymEngine *engine);

#define SYM_SQL_SELECT_FILE_TRIGGERS " select trigger_id, base_dir, recurse,\
 includes_files, excludes_files,\
 sync_on_create, sync_on_modified, sync_on_delete,\
 sync_on_ctl_file, delete_after_sync,\
 before_copy_script,\
 after_copy_script,\
 create_time, last_update_by,\
 last_update_time, channel_id, reload_channel_id\
 from sym_file_trigger"

#define SYM_SQL_TRIGGER_ID_WHERE " where trigger_id=?"

#define SYM_SQL_FILE_TRIGGER_ROUTERS " select\
 tr.trigger_id as trigger_id, tr.router_id as router_id, enabled,\
 initial_load_enabled, target_base_dir,\
 conflict_strategy, tr.create_time as create_time,\
 tr.last_update_by as last_update_by, tr.last_update_time as last_update_time\
 from sym_file_trigger_router tr "

#define SYM_SQL_TRIGGER_ROUTER_ID_WHERE " where trigger_id=? and router_id=?"

#define SYM_SQL_FILE_TRIGGER_ROUTERS_FOR_CURRENT_NODE_WHERE "\
 inner join sym_router r on\
 tr.router_id=r.router_id\
 where r.source_node_group_id=?"

#define SYM_SQL_SELECT_FILE_SNAPSHOT "\
 select trigger_id, router_id, channel_id, reload_channel_id, relative_dir, file_name,\
 last_event_type, crc32_checksum,\
 file_size, file_modified_time, create_time, last_update_time, last_update_by\
 from sym_file_snapshot where trigger_id=? and router_id=? "

#define SYM_SQL_FIND_NODE_ID_FROM_FILE_INCOMING "\
 select node_id from sym_file_incoming where relative_dir=? and file_name=? and file_modified_time=?"

#define SYM_SQL_UPDATE_FILE_SNAPSHOT "\
 update sym_file_snapshot set\
 last_event_type=?, crc32_checksum=?,\
 file_size=?, file_modified_time=?, last_update_time=?,\
 last_update_by=?, channel_id=?, reload_channel_id=?\
 where\
 trigger_id=? and router_id=? and relative_dir=? and file_name=? "

#define SYM_SQL_DELETE_FILE_SNAPSHOT "\
 delete from sym_file_snapshot\
 where\
 trigger_id=? and router_id=? and relative_dir=? and file_name=?  "

#define SYM_SQL_INSERT_FILE_SNAPSHOT "\
 insert into sym_file_snapshot (\
 last_event_type, crc32_checksum,\
 file_size, file_modified_time, create_time, last_update_time,\
 last_update_by, channel_id, reload_channel_id, trigger_id, router_id, relative_dir, file_name\
 ) values(?,?,?,?,?,?,?,?,?,?,?,?,?)                                                             "

#define SYM_SQL_SELECT_FILE_TRIGGER_ROUTERS "\
 select\
  tr.trigger_id as trigger_id, tr.router_id as router_id, enabled,\
  initial_load_enabled, target_base_dir,\
  conflict_strategy, tr.create_time as create_time,\
  tr.last_update_by as last_update_by, tr.last_update_time as last_update_time\
 from sym_file_trigger_router tr "

#define SYM_SQL_WHERE_TRIGGER_ROUTER_ID "where trigger_id=? and router_id=?"

#define SYM_SQL_UPDATE_FILE_INCOMING " update sym_file_incoming set \
                  node_id=?,                                  \
                  file_modified_time=?,                       \
                  last_event_type=?                           \
                 where                                        \
                  relative_dir=? and file_name=? "

#define SYM_SQL_INSERT_FILE_INCOMING "\
         insert into sym_file_incoming (node_id, file_modified_time, last_event_type, relative_dir, file_name) \
           values(?,?,?,?,?) "


#endif
