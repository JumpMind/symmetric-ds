/*
 * Licensed to JumpMind Inc under one or more contributor 
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding 
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Lesser General Public License (the
 * "License"); you may not use this file except in compliance
 * with the License. 
 * 
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see           
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

import org.jumpmind.symmetric.common.logging.ILog;
import org.jumpmind.symmetric.common.logging.LogFactory;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.util.DefaultParameterParser;
import org.jumpmind.symmetric.util.DefaultParameterParser.ParameterMetaData;

/**
 * Constants that represent parameters that can be retrieved or saved via the
 * {@link IParameterService}
 */
final public class ParameterConstants {

    static final ILog log = LogFactory.getLog(ParameterConstants.class);

    public static final String ALL = "ALL";
    
    private static Map<String, ParameterMetaData> parameterMetaData = new DefaultParameterParser().parse();

    private ParameterConstants() {
    }

    public final static String JDBC_EXECUTE_BATCH_SIZE = "db.jdbc.execute.batch.size";

    public final static String START_PULL_JOB = "start.pull.job";
    public final static String START_PUSH_JOB = "start.push.job";
    public final static String START_PURGE_JOB = "start.purge.job";
    public final static String START_ROUTE_JOB = "start.route.job";
    public final static String START_HEARTBEAT_JOB = "start.heartbeat.job";
    public final static String START_SYNCTRIGGERS_JOB = "start.synctriggers.job";
    public final static String START_STATISTIC_FLUSH_JOB = "start.stat.flush.job";
    public final static String START_WATCHDOG_JOB = "start.watchdog.job";

    public final static String JOB_RANDOM_MAX_START_TIME_MS = "job.random.max.start.time.ms";
    public final static String JOB_SYNCTRIGGERS_AFTER_MIDNIGHT_MIN = "job.synctriggers.aftermidnight.minutes";

    public final static String REGISTRATION_NUMBER_OF_ATTEMPTS = "registration.number.of.attempts";

    public final static String REGISTRATION_URL = "registration.url";
    public final static String SYNC_URL = "sync.url";
    public final static String ENGINE_NAME = "engine.name";
    public final static String NODE_GROUP_ID = "group.id";
    public final static String EXTERNAL_ID = "external.id";
    public final static String SCHEMA_VERSION = "schema.version";

    public final static String AUTO_REGISTER_ENABLED = "auto.registration";
    public final static String AUTO_RELOAD_ENABLED = "auto.reload";
    public final static String AUTO_INSERT_REG_SVR_IF_NOT_FOUND = "auto.insert.registration.svr.if.not.found";
    public final static String AUTO_SYNC_CONFIGURATION = "auto.sync.configuration";
    public final static String AUTO_CONFIGURE_DATABASE = "auto.config.database";
    public final static String AUTO_SYNC_TRIGGERS = "auto.sync.triggers";
    public final static String AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT = "auto.config.registration.svr.sql.script";
    public final static String AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML = "auto.config.registration.svr.ddlutil.xml";
    public final static String AUTO_CONFIGURE_EXTRA_TABLES = "auto.config.extra.tables.ddlutil.xml";
    public final static String AUTO_UPGRADE = "auto.upgrade";
    public final static String AUTO_UPDATE_NODE_VALUES = "auto.update.node.values.from.properties";

    public final static String INITIAL_LOAD_DELETE_BEFORE_RELOAD = "initial.load.delete.first";
    public final static String INITIAL_LOAD_DELETE_FIRST_SQL = "initial.load.delete.first.sql";
    public final static String INITIAL_LOAD_CREATE_SCHEMA_BEFORE_RELOAD = "initial.load.create.first";
    public final static String INITIAL_LOAD_USE_RELOAD_CHANNEL = "initial.load.use.reload.channel";

    public final static String STREAM_TO_FILE_ENABLED = "stream.to.file.enabled";
    public final static String STREAM_TO_FILE_THRESHOLD = "stream.to.file.threshold.bytes";

    public final static String PARAMETER_REFRESH_PERIOD_IN_MS = "parameter.reload.timeout.ms";

    public final static String CONCURRENT_WORKERS = "http.concurrent.workers.max";
    public final static String CONCURRENT_RESERVATION_TIMEOUT = "http.concurrent.reservation.timeout.ms";         

    public final static String OUTGOING_BATCH_PEEK_AHEAD_BATCH_COMMIT_SIZE = "outgoing.batches.peek.ahead.batch.commit.size";
    public final static String ROUTING_FLUSH_JDBC_BATCH_SIZE = "routing.flush.jdbc.batch.size";
    public final static String ROUTING_MAX_GAPS_TO_QUALIFY_IN_SQL = "routing.max.gaps.to.qualify.in.sql";
    public final static String ROUTING_PEEK_AHEAD_WINDOW = "routing.peek.ahead.window.after.max.size";
    public final static String ROUTING_STALE_DATA_ID_GAP_TIME = "routing.stale.dataid.gap.time.ms";
    public final static String ROUTING_LARGEST_GAP_SIZE = "routing.largest.gap.size";
    public final static String ROUTING_DATA_READER_TYPE = "routing.data.reader.type";
    public final static String ROUTING_DATA_READER_TYPE_GAP_RETENTION_MINUTES = "routing.data.reader.type.gap.retention.period.minutes";

    public final static String INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED = "incoming.batches.skip.duplicates";
    public final static String DATA_LOADER_ENABLED = "dataloader.enable";
    public final static String DATA_LOADER_NUM_OF_ACK_RETRIES = "num.of.ack.retries";
    public final static String DATA_LOADER_TIME_BETWEEN_ACK_RETRIES = "time.between.ack.retries.ms";
    public final static String DATA_LOADER_NO_KEYS_IN_UPDATE = "dont.include.keys.in.update.statement";
    public final static String DATA_LOADER_ENABLE_FALLBACK_UPDATE = "dataloader.enable.fallback.update";
    public final static String DATA_LOADER_ENABLE_FALLBACK_SAVEPOINT = "dataloader.enable.fallback.savepoint";
    public final static String DATA_LOADER_ENABLE_FALLBACK_INSERT = "dataloader.enable.fallback.insert";
    public final static String DATA_LOADER_ALLOW_MISSING_DELETE = "dataloader.allow.missing.delete";
    public final static String DATA_LOADER_MAX_ROWS_BEFORE_COMMIT = "dataloader.max.rows.before.commit";

    public final static String DATA_RELOAD_IS_BATCH_INSERT_TRANSACTIONAL = "datareload.batch.insert.transactional";

    public final static String DATA_EXTRACTOR_ENABLED = "dataextractor.enable";
    public final static String DATA_EXTRACTOR_FLUSH_FOR_KEEP_ALIVE = "dataextractor.keepalive.period.ms";
    public final static String DATA_EXTRACTOR_OLD_DATA_ENABLED = "dataextractor.old.data.enable";
    public final static String OUTGOING_BATCH_MAX_BATCHES_TO_SELECT = "outgoing.batches.max.to.select";
    
    public final static String DBDIALECT_ORACLE_USE_TRANSACTION_VIEW = "oracle.use.transaction.view";
    
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
    public final static String TRANSPORT_HTTPS_VERIFIED_SERVERS = "https.verified.server.names";
    public final static String TRANSPORT_HTTPS_ALLOW_SELF_SIGNED_CERTS = "https.allow.self.signed.certs";

    public final static String CACHE_TIMEOUT_NODE_SECURITY_IN_MS = "cache.node.security.time.ms";
    public final static String CACHE_TIMEOUT_TRIGGER_ROUTER_IN_MS = "cache.trigger.router.time.ms";
    public final static String CACHE_TIMEOUT_CHANNEL_IN_MS = "cache.channel.time.ms";
    public final static String CACHE_TIMEOUT_TRANSFORM_IN_MS = "cache.transform.time.ms";

    public final static String TRIGGER_UPDATE_CAPTURE_CHANGED_DATA_ONLY = "trigger.update.capture.changed.data.only.enabled";

    public final static String DBPOOL_URL = "db.url";
    public final static String DBPOOL_DRIVER = "db.driver";
    public final static String DBPOOL_USER = "db.user";
    public final static String DBPOOL_PASSWORD = "db.password";
    public final static String DBPOOL_INITIAL_SIZE = "db.pool.initial.size";

    public final static String DB_NATIVE_EXTRACTOR = "db.native.extractor";
    public final static String DB_METADATA_IGNORE_CASE = "db.metadata.ignore.case";
    public final static String DB_QUERY_TIMEOUT_SECS = "db.sql.query.timeout.seconds";

    public final static String RUNTIME_CONFIG_TABLE_PREFIX = "sync.table.prefix";

    public final static String CLUSTER_LOCKING_ENABLED = "cluster.lock.enabled";
    public final static String CLUSTER_LOCK_TIMEOUT_MS = "cluster.lock.timeout.ms";

    public final static String PURGE_RETENTION_MINUTES = "purge.retention.minutes";
    public final static String PURGE_MAX_NUMBER_OF_DATA_IDS = "job.purge.max.num.data.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_BATCH_IDS = "job.purge.max.num.batches.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS = "job.purge.max.num.data.events.to.delete.in.tx";

    public final static String JMX_LINE_FEED = "jmx.line.feed";

    public final static String IP_FILTERS = "ip.filters";

    public final static String WEB_BATCH_SERVLET_ENABLE = "web.batch.servlet.enable";

    public final static String OFFLINE_NODE_DETECTION_PERIOD_MINUTES = "offline.node.detection.period.minutes";
    public final static String HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC = "heartbeat.sync.on.push.period.sec";

    public final static String STATISTIC_RECORD_ENABLE = "statistic.record.enable";
    
    public final static String STORES_UPPERCASE_NAMES_IN_CATALOG = "stores.uppercase.names.in.catalog";
    
    public final static String DB_MASTER_COLLATION = "db.master.collation";
    
    public final static String DB_TREAT_DATE_TIME_AS_VARCHAR = "db.treat.date.time.as.varchar.enabled";

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