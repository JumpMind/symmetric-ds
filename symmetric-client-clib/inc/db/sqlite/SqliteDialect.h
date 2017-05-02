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
#ifndef SYM_SQLITE_DIALECT_H
#define SYM_SQLITE_DIALECT_H

#include <stdio.h>
#include <stdlib.h>
#include "db/SymDialect.h"
#include "common/Log.h"
#include "db/sql/SqlTemplate.h"
#include "db/sqlite/SqliteTriggerTemplate.h"
#include "util/StringArray.h"


typedef struct SymSqliteDialect {
    SymDialect super;
} SymSqliteDialect;

SymSqliteDialect * SymSqliteDialect_new(SymSqliteDialect *this, SymDatabasePlatform *platform);

#define CREATE_SYM_CHANNEL "\
CREATE TABLE sym_channel( \
    channel_id VARCHAR NOT NULL PRIMARY KEY , \
    processing_order INTEGER DEFAULT 1 NOT NULL, \
    max_batch_size INTEGER DEFAULT 1000 NOT NULL, \
    max_batch_to_send INTEGER DEFAULT 60 NOT NULL, \
    max_data_to_route INTEGER DEFAULT 100000 NOT NULL, \
    extract_period_millis INTEGER DEFAULT 0 NOT NULL, \
    enabled INTEGER DEFAULT 1 NOT NULL, \
    use_old_data_to_route INTEGER DEFAULT 1 NOT NULL, \
    use_row_data_to_route INTEGER DEFAULT 1 NOT NULL, \
    use_pk_data_to_route INTEGER DEFAULT 1 NOT NULL, \
    reload_flag INTEGER DEFAULT 0 NOT NULL, \
    file_sync_flag INTEGER DEFAULT 0 NOT NULL, \
    contains_big_lob INTEGER DEFAULT 0 NOT NULL, \
    batch_algorithm VARCHAR DEFAULT 'default' NOT NULL, \
    data_loader_type VARCHAR DEFAULT 'default' NOT NULL, \
    description VARCHAR, \
    create_time TIMESTAMP, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP \
);"

#define CREATE_SYM_DATA "\
CREATE TABLE sym_data( \
    data_id INTEGER NOT NULL PRIMARY KEY  AUTOINCREMENT, \
    table_name VARCHAR NOT NULL, \
    event_type VARCHAR NOT NULL, \
    row_data VARCHAR, \
    pk_data VARCHAR, \
    old_data VARCHAR, \
    trigger_hist_id INTEGER NOT NULL, \
    channel_id VARCHAR, \
    transaction_id VARCHAR, \
    source_node_id VARCHAR, \
    external_data VARCHAR, \
    node_list VARCHAR, \
    create_time TIMESTAMP \
); \
CREATE INDEX sym_idx_d_channel_id ON sym_data (data_id, channel_id); \
"

#define CREATE_SYM_DATA_EVENT "\
CREATE TABLE sym_data_event( \
    data_id INTEGER NOT NULL, \
    batch_id INTEGER NOT NULL, \
    router_id VARCHAR NOT NULL, \
    create_time TIMESTAMP, \
    PRIMARY KEY (data_id, batch_id, router_id) \
); \
CREATE INDEX sym_idx_de_batchid ON sym_data_event (batch_id); \
"

#define CREATE_SYM_INCOMING_BATCH "\
CREATE TABLE sym_incoming_batch( \
    batch_id INTEGER NOT NULL, \
    node_id VARCHAR NOT NULL, \
    channel_id VARCHAR, \
    status VARCHAR, \
    error_flag INTEGER DEFAULT 0, \
    network_millis INTEGER DEFAULT 0 NOT NULL, \
    filter_millis INTEGER DEFAULT 0 NOT NULL, \
    database_millis INTEGER DEFAULT 0 NOT NULL, \
    failed_row_number INTEGER DEFAULT 0 NOT NULL, \
    failed_line_number INTEGER DEFAULT 0 NOT NULL, \
    byte_count INTEGER DEFAULT 0 NOT NULL, \
    statement_count INTEGER DEFAULT 0 NOT NULL, \
    fallback_insert_count INTEGER DEFAULT 0 NOT NULL, \
    fallback_update_count INTEGER DEFAULT 0 NOT NULL, \
    ignore_count INTEGER DEFAULT 0 NOT NULL, \
    missing_delete_count INTEGER DEFAULT 0 NOT NULL, \
    skip_count INTEGER DEFAULT 0 NOT NULL, \
    sql_state VARCHAR, \
    sql_code INTEGER DEFAULT 0 NOT NULL, \
    sql_message VARCHAR, \
    last_update_hostname VARCHAR, \
    last_update_time TIMESTAMP, \
    create_time TIMESTAMP, \
    PRIMARY KEY (batch_id, node_id) \
); \
CREATE INDEX sym_idx_ib_in_error ON sym_incoming_batch (error_flag); \
CREATE INDEX sym_idx_ib_time_status ON sym_incoming_batch (create_time, status); \
"

#define CREATE_SYM_NODE "\
CREATE TABLE sym_node( \
    node_id VARCHAR NOT NULL PRIMARY KEY , \
    node_group_id VARCHAR NOT NULL, \
    external_id VARCHAR NOT NULL, \
    sync_enabled INTEGER DEFAULT 0, \
    sync_url VARCHAR, \
    schema_version VARCHAR, \
    symmetric_version VARCHAR, \
    database_type VARCHAR, \
    database_version VARCHAR, \
    heartbeat_time TIMESTAMP, \
    timezone_offset VARCHAR, \
    batch_to_send_count INTEGER DEFAULT 0, \
    batch_in_error_count INTEGER DEFAULT 0, \
    created_at_node_id VARCHAR, \
    deployment_type VARCHAR \
); \
"

#define CREATE_SYM_NODE_GROUP "\
CREATE TABLE sym_node_group( \
    node_group_id VARCHAR NOT NULL PRIMARY KEY , \
    description VARCHAR, \
    create_time TIMESTAMP, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP \
); \
"

#define CREATE_SYM_NODE_GROUP_LINK "\
CREATE TABLE sym_node_group_link( \
    source_node_group_id VARCHAR NOT NULL, \
    target_node_group_id VARCHAR NOT NULL, \
    data_event_action VARCHAR DEFAULT 'W' NOT NULL, \
    sync_config_enabled INTEGER DEFAULT 1 NOT NULL, \
    create_time TIMESTAMP, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP, \
    PRIMARY KEY (source_node_group_id, target_node_group_id), \
    FOREIGN KEY (target_node_group_id) REFERENCES sym_node_group (node_group_id), \
    FOREIGN KEY (source_node_group_id) REFERENCES sym_node_group (node_group_id) \
); \
"

#define CREATE_SYM_NODE_HOST "\
CREATE TABLE sym_node_host( \
    node_id VARCHAR NOT NULL, \
    host_name VARCHAR NOT NULL, \
    ip_address VARCHAR, \
    os_user VARCHAR, \
    os_name VARCHAR, \
    os_arch VARCHAR, \
    os_version VARCHAR, \
    available_processors INTEGER DEFAULT 0, \
    free_memory_bytes INTEGER DEFAULT 0, \
    total_memory_bytes INTEGER DEFAULT 0, \
    max_memory_bytes INTEGER DEFAULT 0, \
    java_version VARCHAR, \
    java_vendor VARCHAR, \
    jdbc_version VARCHAR, \
    symmetric_version VARCHAR, \
    timezone_offset VARCHAR, \
    heartbeat_time TIMESTAMP, \
    last_restart_time TIMESTAMP NOT NULL, \
    create_time TIMESTAMP NOT NULL, \
    PRIMARY KEY (node_id, host_name) \
); \
"

#define CREATE_SYM_NODE_IDENTITY "\
CREATE TABLE sym_node_identity( \
    node_id VARCHAR NOT NULL PRIMARY KEY , \
    FOREIGN KEY (node_id) REFERENCES sym_node (node_id) \
); \
"

#define CREATE_SYM_NODE_SECURITY "\
CREATE TABLE sym_node_security( \
    node_id VARCHAR NOT NULL PRIMARY KEY , \
    node_password VARCHAR NOT NULL, \
    registration_enabled INTEGER DEFAULT 0, \
    registration_time TIMESTAMP, \
    initial_load_enabled INTEGER DEFAULT 0, \
    initial_load_time TIMESTAMP, \
    initial_load_id INTEGER, \
    initial_load_create_by VARCHAR, \
    rev_initial_load_enabled INTEGER DEFAULT 0, \
    rev_initial_load_time TIMESTAMP, \
    rev_initial_load_id INTEGER, \
    rev_initial_load_create_by VARCHAR, \
    created_at_node_id VARCHAR, \
    FOREIGN KEY (node_id) REFERENCES sym_node (node_id) \
); \
"

#define CREATE_SYM_OUTGOING_BATCH "\
CREATE TABLE sym_outgoing_batch( \
    batch_id INTEGER NOT NULL, \
    node_id VARCHAR NOT NULL, \
    channel_id VARCHAR, \
    status VARCHAR, \
    load_id INTEGER, \
    extract_job_flag INTEGER DEFAULT 0, \
    load_flag INTEGER DEFAULT 0, \
    error_flag INTEGER DEFAULT 0, \
    common_flag INTEGER DEFAULT 0, \
    ignore_count INTEGER DEFAULT 0 NOT NULL, \
    byte_count INTEGER DEFAULT 0 NOT NULL, \
    extract_count INTEGER DEFAULT 0 NOT NULL, \
    sent_count INTEGER DEFAULT 0 NOT NULL, \
    load_count INTEGER DEFAULT 0 NOT NULL, \
    data_event_count INTEGER DEFAULT 0 NOT NULL, \
    reload_event_count INTEGER DEFAULT 0 NOT NULL, \
    insert_event_count INTEGER DEFAULT 0 NOT NULL, \
    update_event_count INTEGER DEFAULT 0 NOT NULL, \
    delete_event_count INTEGER DEFAULT 0 NOT NULL, \
    other_event_count INTEGER DEFAULT 0 NOT NULL, \
    router_millis INTEGER DEFAULT 0 NOT NULL, \
    network_millis INTEGER DEFAULT 0 NOT NULL, \
    filter_millis INTEGER DEFAULT 0 NOT NULL, \
    load_millis INTEGER DEFAULT 0 NOT NULL, \
    extract_millis INTEGER DEFAULT 0 NOT NULL, \
    transform_extract_millis INTEGER DEFAULT 0 NOT NULL, \
    transform_load_millis INTEGER DEFAULT 0 NOT NULL, \
    total_extract_millis INTEGER DEFAULT 0 NOT NULL, \
    total_load_millis INTEGER DEFAULT 0 NOT NULL, \
    sql_state VARCHAR, \
    sql_code INTEGER DEFAULT 0 NOT NULL, \
    sql_message VARCHAR, \
    failed_data_id INTEGER DEFAULT 0 NOT NULL, \
    failed_line_number INTEGER DEFAULT 0 NOT NULL, \
    last_update_hostname VARCHAR, \
    last_update_time TIMESTAMP, \
    create_time TIMESTAMP, \
    create_by VARCHAR, \
    PRIMARY KEY (batch_id, node_id) \
); \
CREATE INDEX sym_idx_ob_in_error ON sym_outgoing_batch (error_flag); \
CREATE INDEX sym_idx_ob_status ON sym_outgoing_batch (status); \
CREATE INDEX sym_idx_ob_node_status ON sym_outgoing_batch (node_id, status); \
"

#define CREATE_SYM_PARAMETER "\
CREATE TABLE sym_parameter( \
    external_id VARCHAR NOT NULL, \
    node_group_id VARCHAR NOT NULL, \
    param_key VARCHAR NOT NULL, \
    param_value VARCHAR, \
    create_time TIMESTAMP, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP, \
    PRIMARY KEY (external_id, node_group_id, param_key) \
); \
"

#define CREATE_SYM_ROUTER "\
CREATE TABLE sym_router( \
    router_id VARCHAR NOT NULL PRIMARY KEY , \
    target_catalog_name VARCHAR, \
    target_schema_name VARCHAR, \
    target_table_name VARCHAR, \
    source_node_group_id VARCHAR NOT NULL, \
    target_node_group_id VARCHAR NOT NULL, \
    router_type VARCHAR, \
    router_expression VARCHAR, \
    sync_on_update INTEGER DEFAULT 1 NOT NULL, \
    sync_on_insert INTEGER DEFAULT 1 NOT NULL, \
    sync_on_delete INTEGER DEFAULT 1 NOT NULL, \
    use_source_catalog_schema INTEGER DEFAULT 1 NOT NULL, \
    create_time TIMESTAMP NOT NULL, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP NOT NULL, \
    FOREIGN KEY (source_node_group_id, target_node_group_id) REFERENCES sym_node_group_link (source_node_group_id, target_node_group_id) \
); \
"

#define CREATE_SYM_SEQUENCE "\
CREATE TABLE sym_sequence( \
    sequence_name VARCHAR NOT NULL PRIMARY KEY , \
    current_value INTEGER DEFAULT 0 NOT NULL, \
    increment_by INTEGER DEFAULT 1 NOT NULL, \
    min_value INTEGER DEFAULT 1 NOT NULL, \
    max_value INTEGER DEFAULT 9999999999 NOT NULL, \
    cycle INTEGER DEFAULT 0, \
    create_time TIMESTAMP, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP NOT NULL \
); \
"

#define CREATE_SYM_TRIGGER "\
CREATE TABLE sym_trigger( \
    trigger_id VARCHAR NOT NULL PRIMARY KEY , \
    source_catalog_name VARCHAR, \
    source_schema_name VARCHAR, \
    source_table_name VARCHAR NOT NULL, \
    channel_id VARCHAR NOT NULL, \
    reload_channel_id VARCHAR DEFAULT 'reload' NOT NULL, \
    sync_on_update INTEGER DEFAULT 1 NOT NULL, \
    sync_on_insert INTEGER DEFAULT 1 NOT NULL, \
    sync_on_delete INTEGER DEFAULT 1 NOT NULL, \
    sync_on_incoming_batch INTEGER DEFAULT 0 NOT NULL, \
    name_for_update_trigger VARCHAR, \
    name_for_insert_trigger VARCHAR, \
    name_for_delete_trigger VARCHAR, \
    sync_on_update_condition VARCHAR, \
    sync_on_insert_condition VARCHAR, \
    sync_on_delete_condition VARCHAR, \
    custom_on_update_text VARCHAR, \
    custom_on_insert_text VARCHAR, \
    custom_on_delete_text VARCHAR, \
    external_select VARCHAR, \
    tx_id_expression VARCHAR, \
    channel_expression VARCHAR, \
    excluded_column_names VARCHAR, \
    sync_key_names VARCHAR, \
    use_stream_lobs INTEGER DEFAULT 0 NOT NULL, \
    use_capture_lobs INTEGER DEFAULT 0 NOT NULL, \
    use_capture_old_data INTEGER DEFAULT 1 NOT NULL, \
    use_handle_key_updates INTEGER DEFAULT 0 NOT NULL, \
    create_time TIMESTAMP NOT NULL, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP NOT NULL, \
    FOREIGN KEY (reload_channel_id) REFERENCES sym_channel (channel_id), \
    FOREIGN KEY (channel_id) REFERENCES sym_channel (channel_id) \
); \
"

#define CREATE_SYM_TRIGGER_HIST "\
CREATE TABLE sym_trigger_hist( \
    trigger_hist_id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT, \
    trigger_id VARCHAR NOT NULL, \
    source_table_name VARCHAR NOT NULL, \
    source_catalog_name VARCHAR, \
    source_schema_name VARCHAR, \
    name_for_update_trigger VARCHAR, \
    name_for_insert_trigger VARCHAR, \
    name_for_delete_trigger VARCHAR, \
    table_hash INTEGER DEFAULT 0 NOT NULL, \
    trigger_row_hash INTEGER DEFAULT 0 NOT NULL, \
    trigger_template_hash INTEGER DEFAULT 0 NOT NULL, \
    column_names VARCHAR NOT NULL, \
    pk_column_names VARCHAR NOT NULL, \
    last_trigger_build_reason VARCHAR NOT NULL, \
    error_message VARCHAR, \
    create_time TIMESTAMP NOT NULL, \
    inactive_time TIMESTAMP \
); \
"

#define CREATE_SYM_TRIGGER_ROUTER "\
CREATE TABLE sym_trigger_router( \
    trigger_id VARCHAR NOT NULL, \
    router_id VARCHAR NOT NULL, \
    enabled INTEGER DEFAULT 1 NOT NULL, \
    initial_load_order INTEGER DEFAULT 1 NOT NULL, \
    initial_load_select VARCHAR, \
    initial_load_delete_stmt VARCHAR, \
    initial_load_batch_count INTEGER DEFAULT 1, \
    ping_back_enabled INTEGER DEFAULT 0 NOT NULL, \
    create_time TIMESTAMP NOT NULL, \
    last_update_by VARCHAR, \
    last_update_time TIMESTAMP NOT NULL, \
    PRIMARY KEY (trigger_id, router_id), \
    FOREIGN KEY (router_id) REFERENCES sym_router (router_id), \
    FOREIGN KEY (trigger_id) REFERENCES sym_trigger (trigger_id) \
); \
"

#define CREATE_SYM_FILE_INCOMING "\
CREATE TABLE sym_file_incoming(             \
    relative_dir VARCHAR NOT NULL,          \
    file_name VARCHAR NOT NULL,             \
    last_event_type VARCHAR NOT NULL,       \
    node_id VARCHAR NOT NULL,               \
    file_modified_time INTEGER,             \
    PRIMARY KEY (relative_dir, file_name)   \
);                                          \
"

#define CREATE_SYM_FILE_SNAPSHOT "\
CREATE TABLE sym_file_snapshot(                                     \
    trigger_id VARCHAR NOT NULL,                                    \
    router_id VARCHAR NOT NULL,                                     \
    relative_dir VARCHAR NOT NULL,                                  \
    file_name VARCHAR NOT NULL,                                     \
    channel_id VARCHAR DEFAULT 'filesync' NOT NULL,                 \
    reload_channel_id VARCHAR DEFAULT 'filesync_reload' NOT NULL,   \
    last_event_type VARCHAR NOT NULL,                               \
    crc32_checksum INTEGER,                                         \
    file_size INTEGER,                                              \
    file_modified_time INTEGER,                                     \
    last_update_time TIMESTAMP NOT NULL,                            \
    last_update_by VARCHAR,                                         \
    create_time TIMESTAMP NOT NULL,                                 \
    PRIMARY KEY (trigger_id, router_id, relative_dir, file_name)    \
);                                                                  \
CREATE INDEX SYM_IDX_F_SNPSHT_CHID ON sym_file_snapshot (reload_channel_id); \
"

#define CREATE_SYM_FILE_TRIGGER "\
CREATE TABLE sym_file_trigger(                                      \
    trigger_id VARCHAR NOT NULL PRIMARY KEY ,                       \
    channel_id VARCHAR DEFAULT 'filesync' NOT NULL,                 \
    reload_channel_id VARCHAR DEFAULT 'filesync_reload' NOT NULL,   \
    base_dir VARCHAR NOT NULL,                                      \
    recurse INTEGER DEFAULT 1 NOT NULL,                             \
    includes_files VARCHAR,                                         \
    excludes_files VARCHAR,                                         \
    sync_on_create INTEGER DEFAULT 1 NOT NULL,                      \
    sync_on_modified INTEGER DEFAULT 1 NOT NULL,                    \
    sync_on_delete INTEGER DEFAULT 1 NOT NULL,                      \
    sync_on_ctl_file INTEGER DEFAULT 0 NOT NULL,                    \
    delete_after_sync INTEGER DEFAULT 0 NOT NULL,                   \
    before_copy_script VARCHAR,                                     \
    after_copy_script VARCHAR,                                      \
    create_time TIMESTAMP NOT NULL,                                 \
    last_update_by VARCHAR,                                         \
    last_update_time TIMESTAMP NOT NULL                             \
);                                                                  \
"

#define CREATE_SYM_FILE_TRIGGER_ROUTER "\
CREATE TABLE sym_file_trigger_router(                                     \
    trigger_id VARCHAR NOT NULL,                                          \
    router_id VARCHAR NOT NULL,                                           \
    enabled INTEGER DEFAULT 1 NOT NULL,                                   \
    initial_load_enabled INTEGER DEFAULT 1 NOT NULL,                      \
    target_base_dir VARCHAR,                                              \
    conflict_strategy VARCHAR DEFAULT 'source_wins' NOT NULL,             \
    create_time TIMESTAMP NOT NULL,                                       \
    last_update_by VARCHAR,                                               \
    last_update_time TIMESTAMP NOT NULL,                                  \
    PRIMARY KEY (trigger_id, router_id),                                  \
    FOREIGN KEY (trigger_id) REFERENCES sym_file_trigger (trigger_id),    \
    FOREIGN KEY (router_id) REFERENCES sym_router (router_id)             \
);                                                                        \
"

#endif
