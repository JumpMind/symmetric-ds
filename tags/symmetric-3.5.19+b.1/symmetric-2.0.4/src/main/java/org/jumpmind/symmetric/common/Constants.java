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

import org.jumpmind.symmetric.model.DataEvent;

final public class Constants {

    private Constants() {
    }

    /**
     * Use this value for the router_id in {@link DataEvent} if the router is unknown.
     */
    public static final String UNKNOWN_ROUTER_ID = "?";
    
    /**
     * Use this value if data is not to be routed anywhere.
     */
    public static final int UNROUTED_BATCH_ID = -1;
    
    public static final String UNROUTED_NODE_ID = "-1";
    
    public static final String ENCODING = "UTF-8";
    
    public static final String OVERRIDE_PROPERTIES_FILE_PREFIX = "symmetric.override.properties.file.";
    
    public static final String OVERRIDE_PROPERTIES_FILE_1 = OVERRIDE_PROPERTIES_FILE_PREFIX + "1";
    
    public static final String OVERRIDE_PROPERTIES_FILE_2 = OVERRIDE_PROPERTIES_FILE_PREFIX + "2";
    
    public static final String OVERRIDE_PROPERTIES_FILE_TEMP = OVERRIDE_PROPERTIES_FILE_PREFIX + "temp";
    
    public static final String NA = "NA";
    
    public static final String SYMMETRIC_ENGINE = "symmetricEngine";
    
    public static final String MBEAN_SERVER = "mbeanserver";
    
    public static final String PROPERTIES = "symmetricProperties";

    public static final String CHANNEL_CONFIG = "config";

    public static final String CHANNEL_RELOAD = "reload";

    public static final String DATA_SOURCE = "dataSource";

    public static final String NODE_SERVICE = "nodeService";
    
    public static final String ROUTER_SERVICE = "routingService";

    public static final String DATALOADER_SERVICE = "dataLoaderService";

    public static final String CLUSTER_SERVICE = "clusterService";

    public static final String PARAMETER_SERVICE = "parameterService";
    
    public static final String TRIGGER_ROUTER_SERVICE = "triggerRouterService";

    public static final String DATALOADER = "dataLoader";

    public static final String INCOMING_BATCH_SERVICE = "incomingBatchService";

    public static final String DATAEXTRACTOR_SERVICE = "dataExtractorService";

    public static final String CONFIG_SERVICE = "configurationService";

    public static final String TRANSPORT_MANAGER = "transportManager";
    
    public static final String EXTENSION_MANAGER = "extensionManager";

    public static final String ACKNOWLEDGE_SERVICE = "acknowledgeService";

    public static final String REGISTRATION_SERVICE = "registrationService";
    
    public static final String UPGRADE_SERVICE = "upgradeService";

    public static final String DATA_SERVICE = "dataService";

    public static final String PUSH_SERVICE = "pushService";

    public static final String PULL_SERVICE = "pullService";
    
    public static final String BANDWIDTH_SERVICE = "bandwidthService";

    public static final String ACK_RESOURCE_HANDLER = "ackResourceHandler";

    public static final String ALERT_RESOURCE_HANDLER = "alertResourceHandler";

    public static final String STATISTIC_MANAGER = "statisticManager";
    
    public static final String STATISTIC_SERVICE = "statisticService";
    
    public static final String SECURITY_SERVICE = "securityService";

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
    
    public static final String JOB_MANAGER = "jobManager";

    public static final String PUSH_JOB_TIMER = "pushJobTimer";

    public static final String PULL_JOB_TIMER = "pullJobTimer";
    
    public static final String ROUTE_JOB_TIMER = "routingJobTimer";

    public static final String PURGE_JOB_TIMER = "purgeJobTimer";

    public static final String HEARTBEAT_JOB_TIMER = "heartbeatJobTimer";

    public static final String SYNC_TRIGGERS_JOB_TIMER = "syncTriggersJobTimer";

    public static final String STATISTIC_FLUSH_JOB_TIMER = "statisticFlushJobTimer";
    
    public static final String WATCHDOG_JOB_TIMER = "watchdogJobTimer";

    public static final String DATA_EXTRACTOR = "dataExtractor";

    public static final String OUTGOING_BATCH_SERVICE = "outgoingBatchService";

    public static final String TRANSACTION_TEMPLATE = "transactionTemplate";

    public static final String PURGE_SERVICE = "purgeService";

    public static final String JDBC_TEMPLATE = "jdbcTemplate";
    
    public static final String PARENT_PROPERTY_PREFIX = "parent.";

    public static final String DOWNLOAD_RATE = "downloadRateKb";

    public static final String MAX_CONCURRENT_WORKERS = "maxConcurrentWorkers";

    public static final String DEFAULT_JMX_SERVER_EXPORTER = "defaultServerExporter";
    
    public static final String PROTOCOL_NONE = "nop";
    
    public static final String PROTOCOL_HTTP = "http";
    
    public static final String PROTOCOL_INTERNAL = "internal";

    public static final String PROTOCOL_EXT = "ext";
    
    public static final String TRANSPORT_HTTPS_VERIFIED_SERVERS_ALL="all";
}
