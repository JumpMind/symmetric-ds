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

import org.jumpmind.symmetric.model.DataEvent;

/**
 * General purpose constants used by SymmetricDS
 */
final public class Constants {

    private Constants() {
    }
    
    public static final long VIRTUAL_BATCH_FOR_REGISTRATION = -9999;   
    
    public static final String NONE_TOKEN = "$(none)";
    
    public static final String DATA_CONTEXT_TARGET_NODE = "targetNode";
    
    public static final String DATA_CONTEXT_SOURCE_NODE = "sourceNode";
        
    public static final String STAGING_CATEGORY_OUTGOING = "outgoing";

    public static final String STAGING_CATEGORY_INCOMING = "incoming";

    public static final String CLIENT_SPRING_XML = "classpath:/symmetric-client.xml";
    
    public static final String SERVER_SPRING_XML = "classpath:/symmetric-server.xml";
    
    public static final String PLEASE_SET_ME = "please set me";
    
    public static final String DEPLOYMENT_TYPE_PROFESSIONAL = "professional";
    
    /**
     * Use this value for the router_id in {@link DataEvent} if the router is unknown.
     */
    public static final String UNKNOWN_ROUTER_ID = "?";
    
    public static final String UNKNOWN_STRING = "unknown";
    
    public static final String ALWAYS_TRUE_CONDITION = "1=1";    
    
    public static final String UNROUTED_NODE_ID = "-1";
    
    public static final long LONG_OPERATION_THRESHOLD = 30000;
    
    public static final String SYMMETRIC_ENGINE = "symmetricEngine";
    
    public static final String MBEAN_SERVER = "mbeanserver";
    
    public static final String CHANNEL_HEARTBEAT = "heartbeat";

    public static final String CHANNEL_CONFIG = "config";
    
    public static final String CHANNEL_RELOAD = "reload";
    
    public static final String CHANNEL_DEFAULT = "default";

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
    
    public static final String SEQUENCE_OUTGOING_BATCH = TableConstants.SYM_OUTGOING_BATCH;
    
    public static final String SEQUENCE_OUTGOING_BATCH_LOAD_ID = "OUTGOING_BATCH_LOAD_ID";
}