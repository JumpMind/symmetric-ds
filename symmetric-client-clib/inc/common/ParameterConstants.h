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

#ifndef SYM_PARAMETER_CONSTANTS_H
#define SYM_PARAMETER_CONSTANTS_H

#define SYM_PARAMETER_ALL "ALL"

#define SYM_PARAMETER_DB_URL "db.url"
#define SYM_PARAMETER_DB_USER "db.user"
#define SYM_PARAMETER_DB_PASSWORD "db.password"

#define SYM_PARAMETER_GROUP_ID "group.id"
#define SYM_PARAMETER_EXTERNAL_ID "external.id"
#define SYM_PARAMETER_SYNC_URL "sync.url"
#define SYM_PARAMETER_REGISTRATION_URL "registration.url"
#define SYM_PARAMETER_SCHEMA_VERSION "schema.version"

#define SYM_PARAMETER_NODE_OFFLINE "node.offline"
#define SYM_PARAMETER_DATA_LOADER_NUM_OF_ACK_RETRIES "num.of.ack.retries"
#define SYM_PARAMETER_DATA_LOADER_TIME_BETWEEN_ACK_RETRIES "time.between.ack.retries.ms"
#define SYM_PARAMETER_DATA_LOADER_IGNORE_MISSING_TABLES "dataloader.ignore.missing.tables"
#define SYM_PARAMETER_DATA_LOADER_USE_PRIMARY_KEYS_FROM_SOURCE "dataloader.use.primary.keys.from.source"
#define SYM_PARAMETER_REGISTRATION_NUMBER_OF_ATTEMPTS "registration.number.of.attempts"
#define SYM_PARAMETER_PARAMETER_REFRESH_PERIOD_IN_MS "parameter.reload.timeout.ms"
#define SYM_PARAMETER_INCOMING_BATCH_RECORD_OK_ENABLED "incoming.batches.record.ok.enabled"
#define SYM_PARAMETER_INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED "incoming.batches.skip.duplicates"
#define SYM_PARAMETER_OUTGOING_BATCH_MAX_BATCHES_TO_SELECT "outgoing.batches.max.to.select"
#define SYM_PARAMETER_TRANSPORT_MAX_BYTES_TO_SYNC "transport.max.bytes.to.sync"
#define SYM_PARAMETER_CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS "cache.trigger.router.time.ms"
#define SYM_PARAMETER_AUTO_SYNC_CONFIGURATION "auto.sync.configuration"
#define SYM_PARAMETER_AUTO_SYNC_CONFIGURATION_ON_INCOMING "auto.sync.configuration.on.incoming"
#define SYM_PARAMETER_AUTO_SYNC_TRIGGERS "auto.sync.triggers"
#define SYM_PARAMETER_AUTO_SYNC_TRIGGERS_AT_STARTUP "auto.sync.triggers.at.startup"

#define SYM_PARAMETER_ROUTING_FLUSH_JDBC_BATCH_SIZE "routing.flush.jdbc.batch.size"

#define SYM_PARAMETER_PURGE_MAX_NUMBER_OF_DATA_IDS "job.purge.max.num.data.to.delete.in.tx"
#define SYM_PARAMETER_PURGE_MAX_NUMBER_OF_BATCH_IDS "job.purge.max.num.batches.to.delete.in.tx"
#define SYM_PARAMETER_PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS "job.purge.max.num.data.event.batches.to.delete.in.tx"
#define SYM_PARAMETER_PURGE_RETENTION_MINUTES "purge.retention.minutes"

#define SYM_PARAMETER_HEARTBEAT_ENABLED "heartbeat.sync.on.push.enabled"
#define SYM_PARAMETER_HEARTBEAT_UPDATE_NODE_WITH_BATCH_STATUS "heartbeat.update.node.with.batch.status"
#define SYM_PARAMETER_AUTO_UPDATE_NODE_VALUES "auto.update.node.values.from.properties"

#define SYM_PARAMETER_CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS "cache.node.group.link.time.ms"

#define SYM_PARAMETER_ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED "routing.data.reader.order.by.gap.id.enabled"

#define SYM_PARAMETER_JOB_MANAGER_SLEEP_PERIOD_MS "job.manager.sleep.period.ms"
#define SYM_PARAMETER_START_ROUTE_JOB "start.route.job"
#define SYM_PARAMETER_START_PUSH_JOB "start.push.job"
#define SYM_PARAMETER_START_PULL_JOB "start.pull.job"
#define SYM_PARAMETER_START_HEARTBEAT_JOB "start.heartbeat.job"
#define SYM_PARAMETER_START_PURGE_JOB "start.purge.job"
#define SYM_PARAMETER_START_SYNCTRIGGERS_JOB "start.synctriggers.job"
#define SYM_PARAMETER_ROUTE_PERIOD_MS "job.routing.period.time.ms"
#define SYM_PARAMETER_PURGE_PERIOD_MS "job.purge.period.time.ms"
#define SYM_PARAMETER_PULL_PERIOD_MS "job.pull.period.time.ms"
#define SYM_PARAMETER_PUSH_PERIOD_MS "job.push.period.time.ms"
#define SYM_PARAMETER_HEARTBEAT_JOB_PERIOD_MS "job.heartbeat.period.time.ms"
#define SYM_PARAMETER_SYNCTRIGGERS_PERIOD_MS "job.synctriggers.period.time.ms"

#define SYM_PARAMETER_NODE_OFFLINE_INCOMING_DIR "node.offline.incoming.dir"
#define SYM_PARAMETER_NODE_OFFLINE_OUTGOING_DIR "node.offline.outgoing.dir"
#define SYM_PARAMETER_NODE_OFFLINE_ERROR_DIR "node.offline.error.dir"
#define SYM_PARAMETER_NODE_OFFLINE_ARCHIVE_DIR "node.offline.archive.dir"
#define SYM_PARAMETER_NODE_OFFLINE_INCOMING_ACCEPT_ALL "node.offline.incoming.accept.all"
#define SYM_PARAMETER_START_OFFLINE_PULL_JOB "start.offline.pull.job"
#define SYM_PARAMETER_START_OFFLINE_PUSH_JOB "start.offline.push.job"
#define SYM_PARAMETER_OFFLINE_PUSH_PERIOD_MS "job.offline.push.period.time.ms"
#define SYM_PARAMETER_OFFLINE_PULL_PERIOD_MS "job.offline.pull.period.time.ms"

#define SYM_PARAMETER_HTTPS_VERIFIED_SERVERS "https.verified.server.names"
#define SYM_PARAMETER_HTTPS_ALLOW_SELF_SIGNED_CERTS "https.allow.self.signed.certs"

#define SYM_PARAMETER_SQLITE_BUSY_TIMEOUT_MS "sqlite.busy.timeout.ms"
#define SYM_PARAMETER_SQLITE_INIT_SQL "sqlite.init.sql"

#define SYM_PARAMETER_FILE_SYNC_ENABLE "file.sync.enable"
#define SYM_PARAMETER_FILE_SYNC_USE_CRC "file.sync.use.crc"
#define SYM_PARAMETER_FILE_SYNC_PREVENT_PING_BACK "file.sync.prevent.ping.back"

#define SYM_PARAMETER_START_FILE_SYNC_TRACKER_JOB "start.file.sync.tracker.job"
#define SYM_PARAMETER_START_FILE_SYNC_PUSH_JOB "start.file.sync.push.job"
#define SYM_PARAMETER_START_FILE_SYNC_PULL_JOB "start.file.sync.pull.job"
#define SYM_PARAMETER_FILE_SYNC_TRACKER_PERIOD_MS "file.tracker.period.minimum.ms"
#define SYM_PARAMETER_FILE_PUSH_MINIMUM_PERIOD_MS "file.push.period.minimum.ms"
#define SYM_PARAMETER_FILE_PULL_MINIMUM_PERIOD_MS "file.pull.period.minimum.ms"

#define SYM_PARAMETER_TRANSPORT_HTTP_TIMEOUT "http.timeout.ms"


#endif
