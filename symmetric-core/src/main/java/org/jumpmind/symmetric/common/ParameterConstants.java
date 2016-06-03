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
package org.jumpmind.symmetric.common;

import java.util.Collection;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.jumpmind.properties.DefaultParameterParser;
import org.jumpmind.properties.DefaultParameterParser.ParameterMetaData;
import org.jumpmind.symmetric.service.IParameterService;

/**
 * Constants that represent parameters that can be retrieved or saved via the
 * {@link IParameterService}
 */
final public class ParameterConstants {

    public static final String ALL = "ALL";

    private static Map<String, ParameterMetaData> parameterMetaData = new DefaultParameterParser("/symmetric-default.properties").parse();

    private ParameterConstants() {
    }
    
    public final static String AUTO_START_ENGINE = "auto.start.engine";

    public final static String JDBC_EXECUTE_BATCH_SIZE = "db.jdbc.execute.batch.size";
    public final static String JDBC_READ_STRINGS_AS_BYTES = "db.read.strings.as.bytes";
    public final static String JDBC_ISOLATION_LEVEL = "db.jdbc.isolation.level";

    public final static String START_PULL_JOB = "start.pull.job";
    public final static String START_PUSH_JOB = "start.push.job";
    public final static String START_PURGE_JOB = "start.purge.job";
    public final static String START_ROUTE_JOB = "start.route.job";
    public final static String START_HEARTBEAT_JOB = "start.heartbeat.job";
    public final static String START_SYNCTRIGGERS_JOB = "start.synctriggers.job";
    public final static String START_STATISTIC_FLUSH_JOB = "start.stat.flush.job";
    public final static String START_STAGE_MGMT_JOB = "start.stage.management.job";
    public final static String START_WATCHDOG_JOB = "start.watchdog.job";

    public final static String PULL_THREAD_COUNT_PER_SERVER = "pull.thread.per.server.count";
    public final static String PULL_MINIMUM_PERIOD_MS = "pull.period.minimum.ms";
    public final static String PULL_LOCK_TIMEOUT_MS = "pull.lock.timeout.ms";

    public final static String PUSH_THREAD_COUNT_PER_SERVER = "push.thread.per.server.count";
    public final static String PUSH_MINIMUM_PERIOD_MS = "push.period.minimum.ms";
    public final static String PUSH_LOCK_TIMEOUT_MS = "push.lock.timeout.ms";

    public final static String OFFLINE_PULL_THREAD_COUNT_PER_SERVER = "offline.pull.thread.per.server.count";
    public final static String OFFLINE_PULL_LOCK_TIMEOUT_MS = "offline.pull.lock.timeout.ms";

    public final static String OFFLINE_PUSH_THREAD_COUNT_PER_SERVER = "offline.push.thread.per.server.count";
    public final static String OFFLINE_PUSH_LOCK_TIMEOUT_MS = "offline.push.lock.timeout.ms";

    public final static String FILE_PULL_THREAD_COUNT_PER_SERVER = "file.pull.thread.per.server.count";
    public final static String FILE_PULL_MINIMUM_PERIOD_MS = "file.pull.period.minimum.ms";
    public final static String FILE_PULL_LOCK_TIMEOUT_MS = "file.pull.lock.timeout.ms";

    public final static String FILE_PUSH_THREAD_COUNT_PER_SERVER = "file.push.thread.per.server.count";
    public final static String FILE_PUSH_MINIMUM_PERIOD_MS = "file.push.period.minimum.ms";
    public final static String FILE_PUSH_LOCK_TIMEOUT_MS = "file.push.lock.timeout.ms";

    public final static String JOB_RANDOM_MAX_START_TIME_MS = "job.random.max.start.time.ms";

    public final static String REGISTRATION_NUMBER_OF_ATTEMPTS = "registration.number.of.attempts";
    public final static String REGISTRATION_REOPEN_USE_SAME_PASSWORD = "registration.reopen.use.same.password";
    public final static String REGISTRATION_REQUIRE_NODE_GROUP_LINK = "registration.require.node.group.link";
    public final static String REGISTRATION_REINITIALIZE_ENABLED = "registration.reinitialize.enable";

    public final static String REGISTRATION_URL = "registration.url";
    public final static String SYNC_URL = "sync.url";
    public final static String ENGINE_NAME = "engine.name";
    public final static String NODE_GROUP_ID = "group.id";
    public final static String EXTERNAL_ID = "external.id";
    public final static String SCHEMA_VERSION = "schema.version";

    public final static String AUTO_REGISTER_ENABLED = "auto.registration";
    public final static String AUTO_RELOAD_ENABLED = "auto.reload";
    public final static String AUTO_RELOAD_REVERSE_ENABLED = "auto.reload.reverse";
    public final static String AUTO_INSERT_REG_SVR_IF_NOT_FOUND = "auto.insert.registration.svr.if.not.found";
    public final static String AUTO_SYNC_CONFIGURATION = "auto.sync.configuration";
    public final static String AUTO_SYNC_CONFIGURATION_ON_INCOMING = "auto.sync.configuration.on.incoming";
    public final static String AUTO_CONFIGURE_DATABASE = "auto.config.database";
    public final static String AUTO_SYNC_TRIGGERS = "auto.sync.triggers";
    public final static String AUTO_SYNC_TRIGGERS_AT_STARTUP = "auto.sync.triggers.at.startup";
    public final static String AUTO_SYNC_TRIGGERS_AFTER_CONFIG_CHANGED = "auto.sync.triggers.after.config.change";
    public final static String AUTO_SYNC_TRIGGERS_AFTER_CONFIG_LOADED = "auto.sync.triggers.after.config.loaded";
    public final static String AUTO_REFRESH_AFTER_CONFIG_CHANGED = "auto.refresh.after.config.changes.detected";
    public final static String AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT = "auto.config.registration.svr.sql.script";
    public final static String AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML = "auto.config.registration.svr.ddlutil.xml";
    public final static String AUTO_CONFIGURE_EXTRA_TABLES = "auto.config.extra.tables.ddlutil.xml";
    public final static String AUTO_UPDATE_NODE_VALUES = "auto.update.node.values.from.properties";

    public final static String INITIAL_LOAD_BLOCK_CHANNELS = "initial.load.block.channels";
    public final static String INITIAL_LOAD_BEFORE_SQL = "initial.load.before.sql";
    public final static String INITIAL_LOAD_AFTER_SQL = "initial.load.after.sql";
    public final static String INITIAL_LOAD_REVERSE_BEFORE_SQL = "initial.load.reverse.before.sql";
    public final static String INITIAL_LOAD_REVERSE_AFTER_SQL = "initial.load.reverse.after.sql";
    public final static String INITIAL_LOAD_DELETE_BEFORE_RELOAD = "initial.load.delete.first";
    public final static String INITIAL_LOAD_DELETE_FIRST_SQL = "initial.load.delete.first.sql";
    public final static String INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD = "initial.load.create.first";
    public final static String INITIAL_LOAD_USE_RELOAD_CHANNEL = "initial.load.use.reload.channel";
    public final static String INITIAL_LOAD_REVERSE_FIRST = "initial.load.reverse.first";
    public final static String INITIAL_LOAD_USE_EXTRACT_JOB = "initial.load.use.extract.job.enabled";
    public final static String INITIAL_LOAD_CONCAT_CSV_IN_SQL_ENABLED = "initial.load.concat.csv.in.sql.enabled";
    public final static String INITIAL_LOAD_EXTRACT_THREAD_COUNT_PER_SERVER = "initial.load.extract.thread.per.server.count";
    public final static String INITIAL_LOAD_EXTRACT_TIMEOUT_MS = "initial.load.extract.timeout.ms";
    public final static String INITIAL_LOAD_EXTRACT_JOB_START = "start.initial.load.extract.job";
    public final static String INITIAL_LOAD_SCHEMA_DUMP_COMMAND = "initial.load.schema.dump.command";
    public final static String INITIAL_LOAD_SCHEMA_LOAD_COMMAND = "initial.load.schema.load.command";
    public final static String INITIAL_LOAD_EXTRACT_AND_SEND_WHEN_STAGED = "initial.load.extract.and.send.when.staged";
    
    public final static String CREATE_TABLE_WITHOUT_DEFAULTS = "create.table.without.defaults";
    public final static String CREATE_TABLE_WITHOUT_FOREIGN_KEYS = "create.table.without.foreign.keys";

    public final static String STREAM_TO_FILE_ENABLED = "stream.to.file.enabled";
    public final static String STREAM_TO_FILE_THRESHOLD = "stream.to.file.threshold.bytes";
    public final static String STREAM_TO_FILE_TIME_TO_LIVE_MS = "stream.to.file.ttl.ms";

    public final static String PARAMETER_REFRESH_PERIOD_IN_MS = "parameter.reload.timeout.ms";

    public final static String CONCURRENT_WORKERS = "http.concurrent.workers.max";
    public final static String CONCURRENT_RESERVATION_TIMEOUT = "http.concurrent.reservation.timeout.ms";

    public final static String OUTGOING_BATCH_PEEK_AHEAD_BATCH_COMMIT_SIZE = "outgoing.batches.peek.ahead.batch.commit.size";
    public final static String ROUTING_FLUSH_JDBC_BATCH_SIZE = "routing.flush.jdbc.batch.size";
    public final static String ROUTING_WAIT_FOR_DATA_TIMEOUT_SECONDS = "routing.wait.for.data.timeout.seconds";
    public final static String ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL = "routing.max.gaps.to.qualify.in.sql";
    public final static String ROUTING_PEEK_AHEAD_MEMORY_THRESHOLD = "routing.peek.ahead.memory.threshold.percent";
    public final static String ROUTING_PEEK_AHEAD_WINDOW = "routing.peek.ahead.window.after.max.size";
    public final static String ROUTING_STALE_DATA_ID_GAP_TIME = "routing.stale.dataid.gap.time.ms";
    public final static String ROUTING_LARGEST_GAP_SIZE = "routing.largest.gap.size";
//    public final static String ROUTING_DATA_READER_TYPE_GAP_RETENTION_MINUTES = "routing.data.reader.type.gap.retention.period.minutes";
    public final static String ROUTING_DATA_READER_ORDER_BY_DATA_ID_ENABLED = "routing.data.reader.order.by.gap.id.enabled";
    public final static String ROUTING_DATA_READER_THRESHOLD_GAPS_TO_USE_GREATER_QUERY = "routing.data.reader.threshold.gaps.to.use.greater.than.query";
    public final static String ROUTING_LOG_STATS_ON_BATCH_ERROR = "routing.log.stats.on.batch.error";

    public final static String INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED = "incoming.batches.skip.duplicates";
    @Deprecated
    public final static String INCOMING_BATCH_DELETE_ON_LOAD = "incoming.batch.delete.on.load";
    public final static String INCOMING_BATCH_RECORD_OK_ENABLED = "incoming.batches.record.ok.enabled";
    public final static String DATA_LOADER_ENABLED = "dataloader.enable";
    public final static String DATA_LOADER_APPLY_CHANGES_ONLY = "dataloader.apply.changes.only";
    public final static String DATA_LOADER_IGNORE_MISSING_TABLES = "dataloader.ignore.missing.tables";
    public final static String DATA_LOADER_FIT_TO_COLUMN = "dataloader.fit.to.column";
    public final static String DATA_LOADER_ERROR_RECORD_CUR_VAL = "dataloader.error.save.curval";
    public final static String DATA_LOADER_NUM_OF_ACK_RETRIES = "num.of.ack.retries";
    public final static String DATA_LOADER_TIME_BETWEEN_ACK_RETRIES = "time.between.ack.retries.ms";
    public final static String DATA_LOADER_MAX_ROWS_BEFORE_COMMIT = "dataloader.max.rows.before.commit";
    public final static String DATA_LOADER_CREATE_TABLE_ALTER_TO_MATCH_DB_CASE = "dataloader.create.table.alter.to.match.db.case";
    public final static String DATA_LOADER_TEXT_COLUMN_EXPRESSION = "dataloader.text.column.expression";
    public final static String DATA_LOADER_SLEEP_TIME_AFTER_EARLY_COMMIT = "dataloader.sleep.time.after.early.commit";
    public final static String DATA_LOADER_TREAT_DATETIME_AS_VARCHAR = "db.treat.date.time.as.varchar.enabled";
    public final static String DATA_LOADER_USE_PRIMARY_KEYS_FROM_SOURCE = "dataloader.use.primary.keys.from.source";

    public final static String DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL = "datareload.batch.insert.transactional";

    public final static String DATA_EXTRACTOR_ENABLED = "dataextractor.enable";
    public final static String DATA_EXTRACTOR_TEXT_COLUMN_EXPRESSION = "dataextractor.text.column.expression";
    public final static String OUTGOING_BATCH_MAX_BATCHES_TO_SELECT = "outgoing.batches.max.to.select";

    public final static String DBDIALECT_ORACLE_USE_TRANSACTION_VIEW = "oracle.use.transaction.view";
    public final static String DBDIALECT_ORACLE_TEMPLATE_NUMBER_SPEC = "oracle.template.precision";
    public final static String DBDIALECT_ORACLE_USE_HINTS = "oracle.use.hints";

    public final static String DBDIALECT_ORACLE_TRANSACTION_VIEW_CLOCK_SYNC_THRESHOLD_MS = "oracle.transaction.view.clock.sync.threshold.ms";

    public final static String DATA_ID_INCREMENT_BY = "data.id.increment.by";

    public final static String TRANSPORT_HTTP_MANUAL_REDIRECTS_ENABLED = "http.manual.redirects.enabled";
    public final static String TRANSPORT_HTTP_TIMEOUT = "http.timeout.ms";
    public final static String TRANSPORT_HTTP_PUSH_STREAM_ENABLED = "http.push.stream.output.enabled";
    public final static String TRANSPORT_HTTP_PUSH_STREAM_SIZE = "http.push.stream.output.size";
    public final static String TRANSPORT_HTTP_USE_COMPRESSION_CLIENT = "http.compression";
    public final static String TRANSPORT_HTTP_COMPRESSION_DISABLED_SERVLET = "web.compression.disabled";
    public final static String TRANSPORT_HTTP_COMPRESSION_LEVEL = "compression.level";
    public final static String TRANSPORT_HTTP_COMPRESSION_STRATEGY = "compression.strategy";
    public final static String TRANSPORT_HTTP_BASIC_AUTH_USERNAME = "http.basic.auth.username";
    public final static String TRANSPORT_HTTP_BASIC_AUTH_PASSWORD = "http.basic.auth.password";
    public final static String TRANSPORT_TYPE = "transport.type";
    public final static String TRANSPORT_MAX_BYTES_TO_SYNC = "transport.max.bytes.to.sync";

    public final static String CACHE_TIMEOUT_GROUPLETS_IN_MS = "cache.grouplets.time.ms";
    public final static String CACHE_TIMEOUT_NODE_SECURITY_IN_MS = "cache.node.security.time.ms";
    public final static String CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS = "cache.trigger.router.time.ms";
    public final static String CACHE_TIMEOUT_CHANNEL_IN_MS = "cache.channel.time.ms";
    public final static String CACHE_TIMEOUT_NODE_GROUP_LINK_IN_MS = "cache.node.group.link.time.ms";
    public final static String CACHE_TIMEOUT_TRANSFORM_IN_MS = "cache.transform.time.ms";
    public final static String CACHE_TIMEOUT_LOAD_FILTER_IN_MS = "cache.load.filter.time.ms";
    public final static String CACHE_TIMEOUT_CONFLICT_IN_MS = "cache.conflict.time.ms";
    public final static String CACHE_TIMEOUT_TABLES_IN_MS = "cache.table.time.ms";

    public final static String TRIGGER_UPDATE_CAPTURE_CHANGED_DATA_ONLY = "trigger.update.capture.changed.data.only.enabled";
    public final static String TRIGGER_CREATE_BEFORE_INITIAL_LOAD = "trigger.create.before.initial.load.enabled";

    public final static String DB_METADATA_IGNORE_CASE = "db.metadata.ignore.case";
    public final static String DB_NATIVE_EXTRACTOR = "db.native.extractor";
    public final static String DB_QUERY_TIMEOUT_SECS = "db.sql.query.timeout.seconds";
    public final static String DB_FETCH_SIZE = "db.jdbc.streaming.results.fetch.size";
    public final static String DB_DELIMITED_IDENTIFIER_MODE = "db.delimited.identifier.mode";
    public final static String DB_JNDI_NAME = "db.jndi.name";
    public final static String DB_SPRING_BEAN_NAME = "db.spring.bean.name";

    public final static String RUNTIME_CONFIG_TABLE_PREFIX = "sync.table.prefix";

    public final static String NODE_ID_CREATOR_SCRIPT = "node.id.creator.script";
    public final static String NODE_ID_CREATOR_MAX_NODES = "node.id.creator.max.nodes";
    
    public final static String EXTERNAL_ID_IS_UNIQUE = "external.id.is.unique.enabled";

    public final static String CLUSTER_SERVER_ID = "cluster.server.id";
    public final static String CLUSTER_LOCKING_ENABLED = "cluster.lock.enabled";
    public final static String CLUSTER_LOCK_TIMEOUT_MS = "cluster.lock.timeout.ms";
    public final static String LOCK_TIMEOUT_MS = "lock.timeout.ms";
    public final static String LOCK_WAIT_RETRY_MILLIS = "lock.wait.retry.ms";

    public final static String PURGE_LOG_SUMMARY_MINUTES = "purge.log.summary.retention.minutes";
    public final static String PURGE_RETENTION_MINUTES = "purge.retention.minutes";
    public final static String PURGE_EXTRACT_REQUESTS_RETENTION_MINUTES = "purge.extract.request.retention.minutes";
    public final static String PURGE_REGISTRATION_REQUEST_RETENTION_MINUTES = "purge.registration.request.retention.minutes";
    public final static String PURGE_STATS_RETENTION_MINUTES = "purge.stats.retention.minutes";
    public final static String PURGE_MAX_NUMBER_OF_DATA_IDS = "job.purge.max.num.data.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_BATCH_IDS = "job.purge.max.num.batches.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS = "job.purge.max.num.data.event.batches.to.delete.in.tx";

    public final static String JMX_LINE_FEED = "jmx.line.feed";

    public final static String IP_FILTERS = "ip.filters";

    public final static String WEB_BATCH_URI_HANDLER_ENABLE = "web.batch.servlet.enable";

    public final static String NODE_COPY_MODE_ENABLED = "node.copy.mode.enabled";

    public final static String NODE_OFFLINE = "node.offline";
    public final static String NODE_OFFLINE_INCOMING_DIR = "node.offline.incoming.dir";
    public final static String NODE_OFFLINE_INCOMING_ACCEPT_ALL = "node.offline.incoming.accept.all";
    public final static String NODE_OFFLINE_OUTGOING_DIR = "node.offline.outgoing.dir";
    public final static String NODE_OFFLINE_ERROR_DIR = "node.offline.error.dir";
    public final static String NODE_OFFLINE_ARCHIVE_DIR = "node.offline.archive.dir";
    
    public final static String OFFLINE_NODE_DETECTION_PERIOD_MINUTES = "offline.node.detection.period.minutes";
    public final static String HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC = "heartbeat.sync.on.push.period.sec";
    public final static String HEARTBEAT_JOB_PERIOD_MS = "job.heartbeat.period.time.ms";
    public final static String HEARTBEAT_SYNC_ON_STARTUP = "heartbeat.sync.on.startup";
    public final static String HEARTBEAT_UPDATE_NODE_WITH_BATCH_STATUS = "heartbeat.update.node.with.batch.status";

    public final static String HEARTBEAT_ENABLED = "heartbeat.sync.on.push.enabled";

    public final static String STATISTIC_RECORD_ENABLE = "statistic.record.enable";

    public final static String CURRENT_ACTIVITY_HISTORY_KEEP_COUNT = "statistic.activity.history.keep.count";

    public final static String STORES_UPPERCASE_NAMES_IN_CATALOG = "stores.uppercase.names.in.catalog";

    public final static String DB_MASTER_COLLATION = "db.master.collation";

    public final static String SEQUENCE_TIMEOUT_MS = "sequence.timeout.ms";

    public final static String REST_API_ENABLED = "rest.api.enable";

    public final static String REST_HEARTBEAT_ON_PULL = "rest.api.heartbeat.on.pull";

    public final static String SYNCHRONIZE_ALL_JOBS = "jobs.synchronized.enable";

    public final static String FILE_SYNC_ENABLE = "file.sync.enable";
    
    public final static String FILE_SYNC_FAST_SCAN = "file.sync.fast.scan";
    
    public final static String FILE_SYNC_USE_CRC = "file.sync.use.crc";
    
    public final static String FILE_SYNC_PREVENT_PING_BACK = "file.sync.prevent.ping.back";

    public final static String FILE_SYNC_LOCK_WAIT_MS = "file.sync.lock.wait.ms";

    public final static String BSH_LOAD_FILTER_HANDLES_MISSING_TABLES = "bsh.load.filter.handles.missing.tables";
    
    public final static String BSH_TRANSFORM_GLOBAL_SCRIPT = "bsh.transform.global.script";
    
    public final static String MSSQL_ROW_LEVEL_LOCKS_ONLY = "mssql.allow.only.row.level.locks.on.runtime.tables";
    
    public final static String MSSQL_USE_NTYPES_FOR_SYNC = "mssql.use.ntypes.for.sync";

    public final static String MSSQL_LOCK_ESCALATION_DISABLED = "mssql.lock.escalation.disabled";

    public final static String MSSQL_INCLUDE_CATALOG_IN_TRIGGERS = "mssql.include.catalog.in.triggers";

    public final static String MSSQL_TRIGGER_EXECUTE_AS = "mssql.trigger.execute.as";
    
    public final static String MSSQL_TRIGGER_ORDER_FIRST = "mssql.trigger.order.first";
    
    public final static String SQLITE_TRIGGER_FUNCTION_TO_USE = "sqlite.trigger.function.to.use";
    
    public final static String AS400_CAST_CLOB_TO = "as400.cast.clob.to";

    public final static String EXTENSIONS_XML = "extensions.xml";
    
    public final static String DATA_CREATE_TIME_TIMEZONE = "data.create_time.timezone";
    
    public final static String LOG_SLOW_SQL_THRESHOLD_MILLIS = "log.slow.sql.threshold.millis";
    
    public final static String LOG_SQL_PARAMETERS_INLINE = "log.sql.parameters.inline";

    public static Map<String, ParameterMetaData> getParameterMetaData() {
        return parameterMetaData;
    }

    public static Set<String> getAllParameterTags() {
        TreeSet<String> tags = new TreeSet<String>();
        Collection<ParameterMetaData> meta = parameterMetaData.values();
        for (ParameterMetaData parameterMetaData : meta) {
            tags.addAll(parameterMetaData.getTags());
        }
        return tags;
    }
}