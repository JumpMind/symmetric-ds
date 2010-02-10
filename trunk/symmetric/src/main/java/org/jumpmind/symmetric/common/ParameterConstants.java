/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Chris Henson <chenson42@users.sourceforge.net>
 *
 * This library is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License as published by the Free Software Foundation; either
 * version 3 of the License, or (at your option) any later version.
 *
 * This library is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 */

package org.jumpmind.symmetric.common;

final public class ParameterConstants {

    private ParameterConstants() {
    }
    
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

    public static final String STATISTIC_THRESHOLD_ALERTS_ENABLED = "statistic.threshold.alerts.enabled";
    public final static String STATISTIC_RETENTION_MINUTES = "statistic.retention.minutes";

    public final static String AUTO_REGISTER_ENABLED = "auto.registration";
    public final static String AUTO_RELOAD_ENABLED = "auto.reload";
    public final static String AUTO_INSERT_REG_SVR_IF_NOT_FOUND = "auto.insert.registration.svr.if.not.found";
    public final static String AUTO_SYNC_CONFIGURATION = "auto.sync.configuration";
    public final static String AUTO_CONFIGURE_DATABASE = "auto.config.database";
    public final static String AUTO_SYNC_TRIGGERS = "auto.sync.triggers";
    public final static String AUTO_CONFIGURE_REG_SVR_SQL_SCRIPT = "auto.config.registration.svr.sql.script";
    public final static String AUTO_CONFIGURE_REG_SVR_DDLUTIL_XML = "auto.config.registration.svr.ddlutil.xml";
    public final static String AUTO_UPGRADE = "auto.upgrade";
    public final static String AUTO_DELETE_BEFORE_RELOAD = "initial.load.delete.first";    
    public final static String AUTO_CREATE_SCHEMA_BEFORE_RELOAD = "initial.load.create.first";
    public final static String AUTO_UPDATE_NODE_VALUES = "auto.update.node.values.from.properties";
    
    public final static String STREAM_TO_FILE_ENABLED = "stream.to.file.enabled";
    public final static String STREAM_TO_FILE_THRESHOLD = "stream.to.file.threshold.bytes";

    public final static String PARAMETER_REFRESH_PERIOD_IN_MS = "parameter.reload.timeout.ms";

    public final static String CONCURRENT_WORKERS = "http.concurrent.workers.max";
    public final static String CONCURRENT_RESERVATION_TIMEOUT = "http.concurrent.reservation.timeout.ms";

    public final static String OUTGOING_BATCH_PEEK_AHEAD_BATCH_COMMIT_SIZE = "outgoing.batches.peek.ahead.batch.commit.size";
    public final static String ROUTING_PEEK_AHEAD_WINDOW = "routing.peek.ahead.window.after.max.size";
    public final static String ROUTING_STALE_DATA_ID_GAP_TIME = "routing.stale.dataid.gap.time.ms";
    public final static String INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED = "incoming.batches.skip.duplicates";
    public final static String DATA_LOADER_ENABLED = "dataloader.enable";
    public final static String DATA_LOADER_NUM_OF_ACK_RETRIES = "num.of.ack.retries";
    public final static String DATA_LOADER_TIME_BETWEEN_ACK_RETRIES = "time.between.ack.retries.ms";
    public final static String DATA_LOADER_NO_KEYS_IN_UPDATE = "dont.include.keys.in.update.statement";
    public final static String DATA_LOADER_ENABLE_FALLBACK_UPDATE = "dataloader.enable.fallback.update";
    public final static String DATA_LOADER_ENABLE_FALLBACK_INSERT = "dataloader.enable.fallback.insert";
    public final static String DATA_LOADER_ALLOW_MISSING_DELETE = "dataloader.allow.missing.delete";
    public final static String DATA_LOADER_MAX_ROWS_BEFORE_COMMIT = "dataloader.max.rows.before.commit";
    
    public final static String DATA_EXTRACTOR_ENABLED = "dataextractor.enable";
    public final static String DATA_EXTRACTOR_OLD_DATA_ENABLED = "dataextractor.old.data.enable";

    public final static String TRANSPORT_HTTP_MANUAL_REDIRECTS_ENABLED = "http.manual.redirects.enabled";
    public final static String TRANSPORT_HTTP_TIMEOUT = "http.timeout.ms";
    public final static String TRANSPORT_HTTP_USE_COMPRESSION_CLIENT = "http.compression";
    public final static String TRANSPORT_HTTP_COMPRESSION_DISABLED_SERVLET = "web.compression.disabled";
    public final static String TRANSPORT_HTTP_COMPRESSION_LEVEL = "compression.level";
    public final static String TRANSPORT_HTTP_COMPRESSION_STRATEGY = "compression.strategy";
    public final static String TRANSPORT_HTTP_BASIC_AUTH_USERNAME = "http.basic.auth.username";
    public final static String TRANSPORT_HTTP_BASIC_AUTH_PASSWORD = "http.basic.auth.password";
    public final static String TRANSPORT_TYPE = "transport.type";
    public final static String TRANSPORT_HTTPS_VERIFIED_SERVERS = "https.verified.server.names";
    public final static String TRANSPORT_HTTPS_ALLOW_SELF_SIGNED_CERTS = "https.allow.self.signed.certs";

    public final static String NODE_SECURITY_CACHE_REFRESH_PERIOD_IN_MS = "cache.node.security.time.ms"; 

    public final static String DBPOOL_URL = "db.url";
    public final static String DBPOOL_DRIVER = "db.driver";
    public final static String DBPOOL_USER = "db.user";
    public final static String DBPOOL_PASSWORD = "db.password";
    public final static String DBPOOL_INITIAL_SIZE = "db.pool.initial.size";

    public final static String DB_NATIVE_EXTRACTOR = "db.native.extractor";
    public final static String DB_METADATA_IGNORE_CASE = "db.metadata.ignore.case";

    public final static String RUNTIME_CONFIG_TABLE_PREFIX = "sync.table.prefix";

    public final static String CLUSTER_LOCKING_ENABLED = "cluster.lock.enabled";
    public final static String CLUSTER_LOCK_TIMEOUT_MS = "cluster.lock.timeout.ms";

    public final static String PURGE_RETENTION_MINUTES = "purge.retention.minutes";
    public final static String PURGE_MAX_NUMBER_OF_DATA_IDS = "job.purge.max.num.data.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_BATCH_IDS = "job.purge.max.num.batches.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_EVENT_BATCH_IDS = "job.purge.max.num.data.events.to.delete.in.tx";

    public final static String JMX_LINE_FEED = "jmx.line.feed";
    public final static String JMX_LEGACY_BEANS_ENABLED = "jmx.legacy.beans.enabled";
    public final static String JMX_HTTP_CONSOLE_ENABLED = "jmx.http.console.for.embedded.webserver.enabled";

    public final static String IP_FILTERS = "ip.filters";
    
    public final static String WEB_BATCH_SERVLET_ENABLE = "web.batch.servlet.enable";
    
    public final static String OFFLINE_NODE_DETECTION_PERIOD_MINUTES = "offline.node.detection.period.minutes";
    public final static String HEARTBEAT_SYNC_ON_PUSH_PERIOD_SEC ="heartbeat.sync.on.push.period.sec";
}