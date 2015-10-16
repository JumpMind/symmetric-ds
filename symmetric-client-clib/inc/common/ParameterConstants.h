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

#ifndef SYM_COMMON_PARAMETERCONSTANTS_H
#define SYM_COMMON_PARAMETERCONSTANTS_H

#define AUTO_START_ENGINE "auto.start.engine"

    #define JDBC_EXECUTE_BATCH_SIZE "db.jdbc.execute.batch.size"
    #define JDBC_READ_STRINGS_AS_BYTES "db.read.strings.as.bytes"

    #define START_PULL_JOB "start.pull.job"
    #define START_PUSH_JOB "start.push.job"
    #define START_PURGE_JOB "start.purge.job"
    #define START_ROUTE_JOB "start.route.job"
    #define START_HEARTBEAT_JOB "start.heartbeat.job"
    #define START_SYNCTRIGGERS_JOB "start.synctriggers.job"
    #define START_STATISTIC_FLUSH_JOB "start.stat.flush.job"
    #define START_STAGE_MGMT_JOB "start.stage.management.job"
    #define START_WATCHDOG_JOB "start.watchdog.job"

    #define PULL_THREAD_COUNT_PER_SERVER "pull.thread.per.server.count"
    #define PULL_MINIMUM_PERIOD_MS "pull.period.minimum.ms"
    #define PULL_LOCK_TIMEOUT_MS "pull.lock.timeout.ms"

    #define PUSH_THREAD_COUNT_PER_SERVER "push.thread.per.server.count"
    #define PUSH_MINIMUM_PERIOD_MS "push.period.minimum.ms"
    #define PUSH_LOCK_TIMEOUT_MS "push.lock.timeout.ms"

    #define OFFLINE_PULL_THREAD_COUNT_PER_SERVER "offline.pull.thread.per.server.count"
    #define OFFLINE_PULL_LOCK_TIMEOUT_MS "offline.pull.lock.timeout.ms"

    #define OFFLINE_PUSH_THREAD_COUNT_PER_SERVER "offline.push.thread.per.server.count"
    #define OFFLINE_PUSH_LOCK_TIMEOUT_MS "offline.push.lock.timeout.ms"

    #define FILE_PULL_THREAD_COUNT_PER_SERVER "file.pull.thread.per.server.count"
    #define FILE_PULL_MINIMUM_PERIOD_MS "file.pull.period.minimum.ms"
    #define FILE_PULL_LOCK_TIMEOUT_MS "file.pull.lock.timeout.ms"

    #define FILE_PUSH_THREAD_COUNT_PER_SERVER "file.push.thread.per.server.count"
    #define FILE_PUSH_MINIMUM_PERIOD_MS "file.push.period.minimum.ms"
    #define FILE_PUSH_LOCK_TIMEOUT_MS "file.push.lock.timeout.ms"

    #define JOB_RANDOM_MAX_START_TIME_MS "job.random.max.start.time.ms"

    #define REGISTRATION_NUMBER_OF_ATTEMPTS "registration.number.of.attempts"
    #define REGISTRATION_REOPEN_USE_SAME_PASSWORD "registration.reopen.use.same.password"
    #define REGISTRATION_REQUIRE_NODE_GROUP_LINK "registration.require.node.group.link"
    #define REGISTRATION_REINITIALIZE_ENABLED "registration.reinitialize.enable"

    #define REGISTRATION_URL "registration.url"
    #define SYNC_URL "sync.url"
    #define ENGINE_NAME "engine.name"
    #define NODE_GROUP_ID "group.id"
    #define EXTERNAL_ID "external.id"
    #define SCHEMA_VERSION "schema.version"

    #define AUTO_REGISTER_ENABLED "auto.registration"
    #define AUTO_RELOAD_ENABLED "auto.reload"
    #define AUTO_RELOAD_REVERSE_ENABLED "auto.reload.reverse"
    #define AUTO_INSERT_REG_SVR_IF_NOT_FOUND "auto.insert.registration.svr.if.not.found"
    #define AUTO_SYNC_CONFIGURATION "auto.sync.configuration"
    #define AUTO_SYNC_CONFIGURATION_ON_INCOMING "auto.sync.configuration.on.incoming"
    #define AUTO_CONFIGURE_DATABASE "auto.config.database"
    #define AUTO_SYNC_TRIGGERS "auto.sync.triggers"
    #define AUTO_SYNC_TRIGGERS_AT_STARTUP "auto.sync.triggers.at.startup"
    #define AUTO_SYNC_TRIGGERS_AFTER_CONFIG_CHANGED "auto.sync.triggers.after.config.change"
    #define AUTO_SYNC_TRIGGERS_AFTER_CONFIG_LOADED "auto.sync.triggers.after.config.loaded"
    #define AUTO_REFRESH_AFTER_CONFIG_CHANGED "auto.refresh.after.config.changes.detected"
    #define AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT "auto.config.registration.svr.sql.script"
    #define AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML "auto.config.registration.svr.ddlutil.xml"
    #define AUTO_CONFIGURE_EXTRA_TABLES "auto.config.extra.tables.ddlutil.xml"
    #define AUTO_UPDATE_NODE_VALUES "auto.update.node.values.from.properties"

    #define INITIAL_LOAD_BEFORE_SQL "initial.load.before.sql"
    #define INITIAL_LOAD_AFTER_SQL "initial.load.after.sql"
    #define INITIAL_LOAD_REVERSE_BEFORE_SQL "initial.load.reverse.before.sql"
    #define INITIAL_LOAD_REVERSE_AFTER_SQL "initial.load.reverse.after.sql"
    #define INITIAL_LOAD_DELETE_BEFORE_RELOAD "initial.load.delete.first"
    #define INITIAL_LOAD_DELETE_FIRST_SQL "initial.load.delete.first.sql"
    #define INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD "initial.load.create.first"
    #define INITIAL_LOAD_USE_RELOAD_CHANNEL "initial.load.use.reload.channel"
    #define INITIAL_LOAD_REVERSE_FIRST "initial.load.reverse.first"
    #define INITIAL_LOAD_USE_EXTRACT_JOB "initial.load.use.extract.job.enabled"
    #define INITIAL_LOAD_CONCAT_CSV_IN_SQL_ENABLED "initial.load.concat.csv.in.sql.enabled"
    #define INITIAL_LOAD_EXTRACT_THREAD_COUNT_PER_SERVER "initial.load.extract.thread.per.server.count"
    #define INITIAL_LOAD_EXTRACT_TIMEOUT_MS "initial.load.extract.timeout.ms"
    #define INITIAL_LOAD_EXTRACT_JOB_START "start.initial.load.extract.job"
    #define INITIAL_LOAD_SCHEMA_DUMP_COMMAND "initial.load.schema.dump.command"
    #define INITIAL_LOAD_SCHEMA_LOAD_COMMAND "initial.load.schema.load.command"

    #define CREATE_TABLE_WITHOUT_DEFAULTS "create.table.without.defaults"
    #define CREATE_TABLE_WITHOUT_FOREIGN_KEYS "create.table.without.foreign.keys"

    #define STREAM_TO_FILE_ENABLED "stream.to.file.enabled"
    #define STREAM_TO_FILE_THRESHOLD "stream.to.file.threshold.bytes"
    #define STREAM_TO_FILE_TIME_TO_LIVE_MS "stream.to.file.ttl.ms"

    #define PARAMETER_REFRESH_PERIOD_IN_MS "parameter.reload.timeout.ms"

    #define CONCURRENT_WORKERS "http.concurrent.workers.max"
    #define CONCURRENT_RESERVATION_TIMEOUT "http.concurrent.reservation.timeout.ms"

    #define OUTGOING_BATCH_PEEK_AHEAD_BATCH_COMMIT_SIZE "outgoing.batches.peek.ahead.batch.commit.size"
    #define ROUTING_FLUSH_JDBC_BATCH_SIZE "routing.flush.jdbc.batch.size"
    #define ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS "routing.wait.for.data.timeout.seconds"
    #define ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL "routing.max.gaps.to.qualify.in.sql"
    #define ROUTING_PEEK_AHEAD_MEMORY_THRESHOLD "routing.peek.ahead.memory.threshold.percent"
    #define ROUTING_PEEK_AHEAD_WINDOW "routing.peek.ahead.window.after.max.size"
    #define ROUTING_STALE_DATA_ID_GAP_TIME "routing.stale.dataid.gap.time.ms"
    #define ROUTING_LARGEST_GAP_SIZE "routing.largest.gap.size"
//    #define ROUTING_DATA_READER_TYPE_GAP_RETENTION_MINUTES "routing.data.reader.type.gap.retention.period.minutes"
    #define ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED "routing.data.reader.order.by.gap.id.enabled"
    #define ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY "routing.data.reader.threshold.gaps.to.use.greater.than.query"
    #define ROUTING_LOG_STATS_ON_BATCH_ERROR "routing.log.stats.on.batch.error"

    #define INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED "incoming.batches.skip.duplicates"
    #define INCOMING_BATCH_RECORD_OK_ENABLED "incoming.batches.record.ok.enabled"
    #define DATA_LOADER_ENABLED "dataloader.enable"
    #define DATA_LOADER_APPLY_CHANGES_ONLY "dataloader.apply.changes.only"
    #define DATA_LOADER_IGNORE_MISSING_TABLES "dataloader.ignore.missing.tables"
    #define DATA_LOADER_FIT_TO_COLUMN "dataloader.fit.to.column"
    #define DATA_LOADER_ERROR_RECORD_CUR_VAL "dataloader.error.save.curval"
    #define DATA_LOADER_NUM_OF_ACK_RETRIES "num.of.ack.retries"
    #define DATA_LOADER_TIME_BETWEEN_ACK_RETRIES "time.between.ack.retries.ms"
    #define DATA_LOADER_MAX_ROWS_BEFORE_COMMIT "dataloader.max.rows.before.commit"
    #define DATA_LOADER_CREATE_TABLE_ALTER_TO_MATCH_DB_CASE "dataloader.create.table.alter.to.match.db.case"
    #define DATA_LOADER_TEXT_COLUMN_EXPRESSION "dataloader.text.column.expression"
    #define DATA_LOADER_SLEEP_TIME_AFTER_EARLY_COMMIT "dataloader.sleep.time.after.early.commit"
    #define DATA_LOADER_TREAT_DATETIME_AS_VARCHAR "db.treat.date.time.as.varchar.enabled"
    #define DATA_LOADER_USE_PRIMARY_KEYS_FROM_SOURCE "dataloader.use.primary.keys.from.source"

    #define DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL "datareload.batch.insert.transactional"

    #define DATA_EXTRACTOR_ENABLED "dataextractor.enable"
    #define DATA_EXTRACTOR_TEXT_COLUMN_EXPRESSION "dataextractor.text.column.expression"
    #define OUTGOING_BATCH_MAX_BATCHES_TO_SELECT "outgoing.batches.max.to.select"

    #define DBDIALECT_ORACLE_USE_TRANSACTION_VIEW "oracle.use.transaction.view"
    #define DBDIALECT_ORACLE_TEMPLATE_NUMBER_SPEC "oracle.template.precision"
    #define DBDIALECT_ORACLE_USE_HINTS "oracle.use.hints"

    #define DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS "oracle.transaction.view.clock.sync.threshold.ms"

    #define DATA_ID_INCREMENT_BY "data.id.increment.by"

    #define TRANSPORT_HTTP_MANUAL_REDIRECTS_ENABLED "http.manual.redirects.enabled"
    #define TRANSPORT_HTTP_TIMEOUT "http.timeout.ms"
    #define TRANSPORT_HTTP_PUSH_STREAM_ENABLED "http.push.stream.output.enabled"
    #define TRANSPORT_HTTP_PUSH_STREAM_SIZE "http.push.stream.output.size"
    #define TRANSPORT_HTTP_USE_COMPRESSION_CLIENT "http.compression"
    #define TRANSPORT_HTTP_COMPRESSION_DISABLED_SERVLET "web.compression.disabled"
    #define TRANSPORT_HTTP_COMPRESSION_LEVEL "compression.level"
    #define TRANSPORT_HTTP_COMPRESSION_STRATEGY "compression.strategy"
    #define TRANSPORT_HTTP_BASIC_AUTH_USERNAME "http.basic.auth.username"
    #define TRANSPORT_HTTP_BASIC_AUTH_PASSWORD "http.basic.auth.password"
    #define TRANSPORT_TYPE "transport.type"
    #define TRANSPORT_MAX_BYTES_TO_SYNC "transport.max.bytes.to.sync"

    #define CACHE_TIMEOUT_GROUPLETS_IN_MS "cache.grouplets.time.ms"
    #define CACHE_TIMEOUT_NODE_SECURITY_IN_MS "cache.node.security.time.ms"
    #define CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS "cache.trigger.router.time.ms"
    #define CACHE_TIMEOUT_CHANNEL_IN_MS "cache.channel.time.ms"
    #define CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS "cache.node.group.link.time.ms"
    #define CACHE_TIMEOUT_TRANSFORM_IN_MS "cache.transform.time.ms"
    #define CACHE_TIMEOUT_LOAD_FILTER_IN_MS "cache.load.filter.time.ms"
    #define CACHE_TIMEOUT_CONFLICT_IN_MS "cache.conflict.time.ms"
    #define CACHE_TIMEOUT_TABLES_IN_MS "cache.table.time.ms"

    #define TRIGGER_UPDATE_CAPTURE_CHANGED_DATA_ONLY "trigger.update.capture.changed.data.only.enabled"
    #define TRIGGER_CREATE_BEFORE_INITIAL_LOAD "trigger.create.before.initial.load.enabled"

    #define DB_METADATA_IGNORE_CASE "db.metadata.ignore.case"
    #define DB_NATIVE_EXTRACTOR "db.native.extractor"
    #define DB_QUERY_TIMEOUT_SECS "db.sql.query.timeout.seconds"
    #define DB_FETCH_SIZE "db.jdbc.streaming.results.fetch.size"
    #define DB_DELIMITED_IDENTIFIER_MODE "db.delimited.identifier.mode"
    #define DB_JNDI_NAME "db.jndi.name"
    #define DB_SPRING_BEAN_NAME "db.spring.bean.name"

    #define RUNTIME_CONFIG_TABLE_PREFIX "sync.table.prefix"

    #define NODE_ID_CREATOR_SCRIPT "node.id.creator.script"
    #define NODE_ID_CREATOR_MAX_NODES "node.id.creator.max.nodes"

    #define EXTERNAL_ID_IS_UNIQUE "external.id.is.unique.enabled"

    #define CLUSTER_SERVER_ID "cluster.server.id"
    #define CLUSTER_LOCKING_ENABLED "cluster.lock.enabled"
    #define CLUSTER_LOCK_TIMEOUT_MS "cluster.lock.timeout.ms"
    #define LOCK_TIMEOUT_MS "lock.timeout.ms"
    #define LOCK_WAIT_RETRY_MILLIS "lock.wait.retry.ms"

    #define PURGE_LOG_SUMMARY_MINUTES "purge.log.summary.retention.minutes"
    #define PURGE_RETENTION_MINUTES "purge.retention.minutes"
    #define PURGE_EXTRACT_REQUESTS_RETENTION_MINUTES "purge.extract.request.retention.minutes"
    #define PURGE_REGISTRATION_REQUEST_RETENTION_MINUTES "purge.registration.request.retention.minutes"
    #define PURGE_STATS_RETENTION_MINUTES "purge.stats.retention.minutes"
    #define PURGE_MAX_NUMBER_OF_DATA_IDS "job.purge.max.num.data.to.delete.in.tx"
    #define PURGE_MAX_NUMBER_OF_BATCH_IDS "job.purge.max.num.batches.to.delete.in.tx"
    #define PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS "job.purge.max.num.data.event.batches.to.delete.in.tx"

    #define JMX_LINE_FEED "jmx.line.feed"

    #define IP_FILTERS "ip.filters"

    #define WEB_BATCH_URI_HANDLER_ENABLE "web.batch.servlet.enable"

    #define NODE_COPY_MODE_ENABLED "node.copy.mode.enabled"

    #define NODE_OFFLINE "node.offline"
    #define NODE_OFFLINE_INCOMING_DIR "node.offline.incoming.dir"
    #define NODE_OFFLINE_INCOMING_ACCEPT_ALL "node.offline.incoming.accept.all"
    #define NODE_OFFLINE_OUTGOING_DIR "node.offline.outgoing.dir"
    #define NODE_OFFLINE_ERROR_DIR "node.offline.error.dir"
    #define NODE_OFFLINE_ARCHIVE_DIR "node.offline.archive.dir"

    #define OFFLINE_NODE_DETECTION_PERIOD_MINUTES "offline.node.detection.period.minutes"
    #define HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC "heartbeat.sync.on.push.period.sec"
    #define HEARTBEAT_JOB_PERIOD_MS "job.heartbeat.period.time.ms"
    #define HEARTBEAT_SYNC_ON_STARTUP "heartbeat.sync.on.startup"
    #define HEARTBEAT_UPDATE_NODE_WITH_BATCH_STATUS "heartbeat.update.node.with.batch.status"

    #define HEARTBEAT_ENABLED "heartbeat.sync.on.push.enabled"

    #define STATISTIC_RECORD_ENABLE "statistic.record.enable"

    #define CURRENT_ACTIVITY_HISTORY_KEEP_COUNT "statistic.activity.history.keep.count"

    #define STORES_UPPERCASE_NAMES_IN_CATALOG "stores.uppercase.names.in.catalog"

    #define DB_MASTER_COLLATION "db.master.collation"

    #define SEQUENCE_TIMEOUT_MS "sequence.timeout.ms"

    #define REST_API_ENABLED "rest.api.enable"

    #define REST_HEARTBEAT_ON_PULL "rest.api.heartbeat.on.pull"

    #define SYNCHRONIZE_ALL_JOBS "jobs.synchronized.enable"

    #define FILE_SYNC_ENABLE "file.sync.enable"

    #define FILE_SYNC_LOCK_WAIT_MS "file.sync.lock.wait.ms"

    #define BSH_LOAD_FILTER_HANDLES_MISSING_TABLES "bsh.load.filter.handles.missing.tables"

    #define BSH_TRANSFORM_GLOBAL_SCRIPT "bsh.transform.global.script"

    #define MSSQL_ROW_LEVEL_LOCKS_ONLY "mssql.allow.only.row.level.locks.on.runtime.tables"

    #define MSSQL_USE_NTYPES_FOR_SYNC "mssql.use.ntypes.for.sync"


    #define MSSQL_TRIGGER_EXECUTE_AS "mssql.trigger.execute.as"

    #define SQLITE_TRIGGER_FUNCTION_TO_USE "sqlite.trigger.function.to.use"

    #define EXTENSIONS_XML "extensions.xml"


#endif /* SYM_COMMON_PARAMETERCONSTANTS_H */
