/*
 * SymmetricDS is an open source database synchronization solution.
 *   
 * Copyright (C) Eric Long <erilong@users.sourceforge.net>,
 *               Chris Henson <chenson42@users.sourceforge.net>
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

public class Constants
{

    public static final String OVERRIDE_PROPERTIES_FILE_1 = "symmetric.override.properties.file.1";
    
    public static final String OVERRIDE_PROPERTIES_FILE_2 = "symmetric.override.properties.file.2";
    
    public static final String MBEAN_SERVER = "mbeanserver";
    
    public static final String PROPERTIES = "properties";

    public static final String CHANNEL_CONFIG = "config";

    public static final String CHANNEL_RELOAD = "reload";

    public static final String DATA_SOURCE = "dataSource";

    public static final String BOOTSTRAP_SERVICE = "bootstrapService";

    public static final String NODE_SERVICE = "nodeService";

    public static final String DATALOADER_SERVICE = "dataLoaderService";

    public static final String CLUSTER_SERVICE = "clusterService";

    public static final String PARAMETER_SERVICE = "parameterService";

    public static final String DATALOADER = "dataLoader";

    public static final String INCOMING_BATCH_SERVICE = "incomingBatchService";

    public static final String DATAEXTRACTOR_SERVICE = "dataExtractorService";

    public static final String CONFIG_SERVICE = "configurationService";

    public static final String TRANSPORT_MANAGER = "transportManager";

    public static final String ACKNOWLEDGE_SERVICE = "acknowledgeService";

    public static final String REGISTRATION_SERVICE = "registrationService";

    public static final String DATA_SERVICE = "dataService";

    public static final String PUSH_SERVICE = "pushService";

    public static final String PULL_SERVICE = "pullService";

    public static final String ACK_RESOURCE_HANDLER = "ackResourceHandler";

    public static final String ALERT_RESOURCE_HANDLER = "alertResourceHandler";

    public static final String STATISTIC_MANAGER = "statisticManager";
    
    public static final String STATISTIC_SERVICE = "statisticService";

    public static final String AUTHENTICATION_RESOURCE_HANDLER = "authenticationResourceHandler";

    public static final String PULL_RESOURCE_HANDLER = "pullResourceHandler";

    public static final String PUSH_RESOURCE_HANDLER = "pushResourceHandler";

    public static final String REGISTRATION_RESOURCE_HANDLER = "registrationResourceHandler";

    public static final String ACK_SERVLET = "ackResourceHandler";

    public static final String ALERT_SERVLET = "alertResourceHandler";

    public static final String PULL_SERVLET = "pullResourceServlet";

    public static final String INET_ADDRESS_FILTER = "inetAddressFilter";

    public static final String INET_ADDRESS_RESOURCE_HANDLER = "inetAddressResourceHandler";

    public static final String PUSH_SERVLET = "pushResourceServlet";

    public static final String REGISTRATION_SERVLET = "registrationServlet";

    public static final String AUTHENTICATION_FILTER = "authenticationFilter";

    public static final String THROTTLE_FILTER = "throttleFilter";

    public static final String COMPRESSION_FILTER = "compressionFilter";

    public static final String NODE_CONCURRENCY_FILTER = "nodeConcurrencyFilter";

    public static final String CONCURRENT_CONNECTION_MANGER = "concurrentConnectionManager";

    public static final String DB_DIALECT = "dbDialect";

    public static final String PUSH_JOB_TIMER = "pushJobTimer";

    public static final String PULL_JOB_TIMER = "pullJobTimer";

    public static final String PURGE_JOB_TIMER = "purgeJobTimer";

    public static final String HEARTBEAT_JOB_TIMER = "heartbeatJobTimer";

    public static final String SYNC_TRIGGERS_JOB_TIMER = "syncTriggersJobTimer";

    public static final String STATISTIC_FLUSH_JOB_TIMER = "statisticFlushJobTimer";

    public static final String DATA_EXTRACTOR = "dataExtractor";

    public static final String OUTGOING_BATCH_SERVICE = "outgoingBatchService";

    public static final String OUTGOING_BATCH_HISTORY_SERVICE = "outgoingBatchHistoryService";

    public static final String PURGE_SERVICE = "purgeService";

    public static final String JDBC = "jdbcTemplate";

    public static final String DOWNLOAD_RATE = "downloadRateKb";

    public static final String MAX_CONCURRENT_WORKERS = "maxConcurrentWorkers";

    public static final String DEFAULT_JMX_SERVER_EXPORTER = "defaultServerExporter";
    
    public static final String PROTOCOL_NONE = "nop";
    
    public static final String PROTOCOL_HTTP = "http";
    
    public static final String PROTOCOL_INTERNAL = "internal";

}
