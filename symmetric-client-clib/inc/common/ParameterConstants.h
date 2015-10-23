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

#define SYM_PURGE_MAX_NUMBER_OF_DATA_IDS "job.purge.max.num.data.to.delete.in.tx"
#define SYM_PURGE_MAX_NUMBER_OF_BATCH_IDS "job.purge.max.num.batches.to.delete.in.tx"
#define SYM_PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS "job.purge.max.num.data.event.batches.to.delete.in.tx"
#define SYM_PURGE_RETENTION_MINUTES "purge.retention.minutes"

#define SYM_PARAMETER_CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS "cache.node.group.link.time.ms"

#endif
