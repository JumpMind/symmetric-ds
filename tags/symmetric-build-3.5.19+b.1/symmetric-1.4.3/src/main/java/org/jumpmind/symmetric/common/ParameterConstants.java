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

public class ParameterConstants {

    public final static String START_PULL_JOB = "start.pull.job";
    public final static String START_PUSH_JOB = "start.push.job";
    public final static String START_PURGE_JOB = "start.purge.job";
    public final static String START_HEARTBEAT_JOB = "start.heartbeat.job";
    public final static String START_SYNCTRIGGERS_JOB = "start.synctriggers.job";
    public final static String START_STATISTIC_FLUSH_JOB = "start.stat.flush.job";

    public final static String JOB_RANDOM_MAX_START_TIME_MS = "job.random.max.start.time.ms";
    public final static String JOB_SYNCTRIGGERS_AFTER_MIDNIGHT_MIN = "job.synctriggers.aftermidnight.minutes";

    public final static String REGISTRATION_URL = "registration.url";
    public final static String MY_URL = "my.url";
    public final static String ENGINE_NAME = "engine.name";
    public final static String NODE_GROUP_ID = "group.id";
    public final static String EXTERNAL_ID = "external.id";
    public final static String SCHEMA_VERSION = "schema.version";

    public static final String STATISTIC_THRESHOLD_ALERTS_ENABLED = "statistic.threshold.alerts.enabled";

    @Deprecated
    public final static String RUNTIME_CONFIGURATION_CLASS = "configuration.class";

    public final static String AUTO_REGISTER_ENABLED = "auto.registration";
    public final static String AUTO_RELOAD_ENABLED = "auto.reload";
    public final static String AUTO_CONFIGURE_DATABASE = "auto.config.database";
    public final static String AUTO_CONFIGURE_REGISTRATION_SERVER_SQL_SCRIPT = "auto.config.registration.svr.sql.script";
    public final static String AUTO_UPGRADE = "auto.upgrade";
    public final static String AUTO_DELETE_BEFORE_RELOAD = "initial.load.delete.first";
    public final static String AUTO_CREATE_SCHEMA_BEFORE_RELOAD = "initial.load.create.first";

    public final static String PARAMETER_REFRESH_PERIOD_IN_MS = "parameter.reload.timeout.ms";

    public final static String CONCURRENT_WORKERS = "http.concurrent.workers.max";
    public final static String CONCURRENT_RESERVATION_TIMEOUT = "http.concurrent.reservation.timeout.ms";

    public final static String OUTGOING_BATCH_PEEK_AHEAD_BATCH_COMMIT_SIZE = "outgoing.batches.peek.ahead.batch.commit.size";
    public final static String OUTGOING_BATCH_PEEK_AHEAD_WINDOW = "outgoing.batches.peek.ahead.window.after.max.size";
    public final static String INCOMING_BATCH_SKIP_DUPLICATE_BATCHES_ENABLED = "incoming.batches.skip.duplicates";
    public final static String DATA_LOADER_NUM_OF_ACK_RETRIES = "num.of.ack.retries";
    public final static String DATA_LOADER_TIME_BETWEEN_ACK_RETRIES = "time.between.ack.retries.ms";
    public final static String DATA_LOADER_NO_KEYS_IN_UPDATE = "dont.include.keys.in.update.statement";
    public final static String DATA_LOADER_LOOKUP_TARGET_SCHEMA = "dataloader.lookup.target.schema";
    public final static String DATA_LOADER_ENABLE_FALLBACK_UPDATE = "dataloader.enable.fallback.update";
    public final static String DATA_LOADER_ENABLE_FALLBACK_INSERT = "dataloader.enable.fallback.insert";
    public final static String DATA_LOADER_ALLOW_MISSING_DELETE = "dataloader.allow.missing.delete";

    public final static String TRANSPORT_HTTP_TIMEOUT = "http.timeout.ms";
    public final static String TRANSPORT_HTTP_USE_COMPRESSION_CLIENT = "http.compression";
    public final static String TRANSPORT_HTTP_COMPRESSION_DISABLED_SERVLET = "web.compression.disabled";
    public final static String TRANSPORT_TYPE = "transport.type";
    public final static String TRANSPORT_HTTPS_VERIFIED_SERVERS = "https.verified.server.names";
    
    public final static String NODE_SECURITY_CACHE_REFRESH_PERIOD_IN_MS = "cache.node.security.time.ms"; 

    public final static String DBPOOL_URL = "db.url";
    public final static String DBPOOL_DRIVER = "db.driver";
    public final static String DBPOOL_USER = "db.user";
    public final static String DBPOOL_PASSWORD = "db.password";
    public final static String DBPOOL_INITIAL_SIZE = "db.pool.initial.size";

    public final static String RUNTIME_CONFIG_TABLE_PREFIX = "sync.table.prefix";

    public final static String CLUSTER_LOCK_TIMEOUT_MS = "cluster.lock.timeout.ms";
    public final static String CLUSTER_LOCK_DURING_PURGE = "cluster.lock.during.purge";
    public final static String CLUSTER_LOCK_DURING_PULL = "cluster.lock.during.pull";
    public final static String CLUSTER_LOCK_DURING_PUSH = "cluster.lock.during.push";
    public final static String CLUSTER_LOCK_DURING_HEARTBEAT = "cluster.lock.during.heartbeat";
    public final static String CLUSTER_LOCK_DURING_SYNC_TRIGGERS = "cluster.lock.during.sync.triggers";

    public final static String PURGE_RETENTION_MINUTES = "purge.retention.minutes";
    public final static String PURGE_MAX_NUMBER_OF_DATA_IDS = "job.purge.max.num.data.events.to.delete.in.tx";
    public final static String PURGE_MAX_NUMBER_OF_BATCH_IDS = "job.purge.max.num.batches.to.delete.in.tx";

    public final static String JMX_LINE_FEED = "jmx.line.feed";
    public final static String JMX_LEGACY_BEANS_ENABLED = "jmx.legacy.beans.enabled";
    public final static String JMX_HTTP_CONSOLE_ENABLED = "jmx.http.console.for.embedded.webserver.enabled";

    public final static String IP_FILTERS = "ip.filters";
    
    public final static String WEB_BATCH_SERVLET_ENABLE = "web.batch.servlet.enable";
}