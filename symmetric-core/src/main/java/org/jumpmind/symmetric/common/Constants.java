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
 * under the License.  */
package org.jumpmind.symmetric.common;

/**
 * 
 */
final public class Constants {

    private Constants() {
    }
        
    public static final String CLIENT_SPRING_XML = "classpath:/symmetric-client.xml";
    
    public static final String SERVER_SPRING_XML = "classpath:/symmetric-server.xml";
    
    public static final String SYS_PROP_STANDALONE_WEB = "symmetric.standalone.web";
    
    public static final String SYS_PROP_ENGINES_DIR = "symmetric.engines.dir";
    
    public static final String SYS_PROP_WEB_DIR = "symmetric.default.web.dir";
    
    public static final String SYS_PROP_DEFAULT_HTTP_PORT = "symmetric.default.http.port";
    
    public static final String SYS_PROP_DEFAULT_HTTPS_PORT = "symmetric.default.https.port";
    
    public static final String SYS_PROP_CREATE_JMX_SERVER = "symmetric.default.create.jmx.server";

    public static final String PLEASE_SET_ME = "please set me";
    
    /**
     * Use this value for the router_id in {@link DataEvent} if the router is unknown.
     */
    public static final String UNKNOWN_ROUTER_ID = "?";
    
    public static final String UNKNOWN_STRING = "unknown";
    
    public static final String ALWAYS_TRUE_CONDITION = "1=1";
    
    /**
     * Use this value if data is not to be routed anywhere.
     */
    public static final int UNROUTED_BATCH_ID = -1;
    
    public static final String UNROUTED_NODE_ID = "-1";
    
    public static final long LONG_OPERATION_THRESHOLD = 30000;
    
    public static final String ENCODING = "UTF-8";    
    
    public static final String OVERRIDE_PROPERTIES_FILE_PREFIX = "symmetric.override.properties.file.";
    
    public static final String OVERRIDE_PROPERTIES_FILE_1 = OVERRIDE_PROPERTIES_FILE_PREFIX + "1";
    
    public static final String OVERRIDE_PROPERTIES_FILE_2 = OVERRIDE_PROPERTIES_FILE_PREFIX + "2";
    
    public static final String OVERRIDE_PROPERTIES_FILE_TEMP = OVERRIDE_PROPERTIES_FILE_PREFIX + "temp";
    
    public static final String SYMMETRIC_ENGINE = "symmetricEngine";
    
    public static final String MBEAN_SERVER = "mbeanserver";
    
    public static final String PROPERTIES = "symmetricProperties";

    public static final String CHANNEL_CONFIG = "config";
    
    public static final String CHANNEL_RELOAD = "reload";
    
    public static final String CHANNEL_DEFAULT = "default";

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

    public static final String ACK_RESOURCE_HANDLER = "ackUriHandler";

    public static final String ALERT_RESOURCE_HANDLER = "alertUriHandler";

    public static final String STATISTIC_MANAGER = "statisticManager";
    
    public static final String STATISTIC_SERVICE = "statisticService";
    
    public static final String SECURITY_SERVICE = "securityService";

    public static final String PULL_URI_HANDLER = "pullUriHandler";

    public static final String PUSH_URI_HANDLER = "pushUriHandler";

    public static final String REGISTRATION_URI_HANDLER = "registrationUriHandler";

    public static final String AUTHENTICATION_INTERCEPTOR = "authenticationInterceptor";

    public static final String NODE_CONCURRENCY_INTERCEPTOR = "nodeConcurrencyInterceptor";
    
    public static final String DEPLOYMENT_TYPE = "deploymentType";

    public static final String CONCURRENT_CONNECTION_MANGER = "concurrentConnectionManager";

    public static final String DB_DIALECT = "dbDialect";
    
    public static final String JOB_MANAGER = "jobManager";

    public static final String PUSH_JOB_TIMER = "job.push";

    public static final String PULL_JOB_TIMER = "job.pull";
    
    public static final String ROUTE_JOB_TIMER = "job.routing";

    public static final String PURGE_JOB_TIMER = "job.purge";

    public static final String HEARTBEAT_JOB_TIMER = "job.heartbeat";

    public static final String SYNC_TRIGGERS_JOB_TIMER = "job.synctriggers";

    public static final String STATISTIC_FLUSH_JOB_TIMER = "job.stat.flush";
    
    public static final String WATCHDOG_JOB_TIMER = "job.watchdog";

    public static final String DATA_EXTRACTOR = "dataExtractor";

    public static final String OUTGOING_BATCH_SERVICE = "outgoingBatchService";

    public static final String TRANSACTION_TEMPLATE = "transactionTemplate";

    public static final String PURGE_SERVICE = "purgeService";

    public static final String JDBC_TEMPLATE = "jdbcTemplate";
    
    public static final String PARENT_PROPERTY_PREFIX = "parent.";

    public static final String DOWNLOAD_RATE = "downloadRateKb";

    public static final String MAX_CONCURRENT_WORKERS = "maxConcurrentWorkers";

    public static final String PROTOCOL_NONE = "nop";
    
    public static final String PROTOCOL_HTTP = "http";
    
    public static final String PROTOCOL_INTERNAL = "internal";

    public static final String PROTOCOL_EXT = "ext";
    
    public static final String TRANSPORT_HTTPS_VERIFIED_SERVERS_ALL="all";
    
    public static final String TRANSFORM_SERVICE = "transformService";
}