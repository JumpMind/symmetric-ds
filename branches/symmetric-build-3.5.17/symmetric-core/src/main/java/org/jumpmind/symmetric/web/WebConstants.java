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
package org.jumpmind.symmetric.web;

/**
 * Constants that are related to the HTTP transport
 */
public class WebConstants {

    public static final String HEADER_ACCEPT_CHARSET = "Accept-Charset";
    
    public static final String METHOD_GET = "GET";
    
    public static final String METHOD_HEAD = "HEAD";
    
    public static final String INIT_PARAM_AUTO_START = "autoStart";
    
    public static final String INIT_PARAM_AUTO_CREATE = "autoCreate";
    
    public static final String INIT_PARAM_MULTI_SERVER_MODE = "multiServerMode";
    
    public static final String INIT_PARAM_STATIC_ENGINES_MODE = "staticEnginesMode";
    
    public static final String INIT_PARAM_DEPLOYMENT_TYPE = "deploymentType";
    
    public static final String INIT_SINGLE_SERVER_PROPERTIES_FILE = "singleServerPropertiesFile";
    
    public static final String ATTR_ENGINE_HOLDER = "symmetricEngineHolder";

    public static final int REGISTRATION_NOT_OPEN = 656;

    public static final int REGISTRATION_REQUIRED = 657;
    
    public static final int SYNC_DISABLED = 658;
    
    public static final int SC_FORBIDDEN = 403;
    
    public static final int SC_SERVICE_UNAVAILABLE = 503;

    public static final String ACK_BATCH_NAME = "batch-";

    public static final String ACK_BATCH_OK = "ok";

    public static final String ACK_NODE_ID = "nodeId-";

    public static final String ACK_NETWORK_MILLIS = "network-";

    public static final String ACK_FILTER_MILLIS = "filter-";

    public static final String ACK_DATABASE_MILLIS = "database-";

    public static final String ACK_BYTE_COUNT = "byteCount-";
    
    public static final String ACK_IGNORE_COUNT = "ignoreCount-";

    public static final String ACK_SQL_STATE = "sqlState-";

    public static final String ACK_SQL_CODE = "sqlCode-";

    public static final String ACK_SQL_MESSAGE = "sqlMessage-";

    public static final String NODE_ID = "nodeId";

    public static final String NODE_GROUP_ID = "nodeGroupId";

    public static final String EXTERNAL_ID = "externalId";

    public static final String SYMMETRIC_VERSION = "symmetricVersion";
    
    public static final String HOST_NAME = "hostName";
    
    public static final String IP_ADDRESS = "ipAddress";

    public static final String SYNC_URL = "syncURL";

    public static final String SCHEMA_VERSION = "schemaVersion";

    public static final String DATABASE_TYPE = "databaseType";

    public static final String DATABASE_VERSION = "databaseVersion";

    public static final String SECURITY_TOKEN = "securityToken";

    public static final String SUSPENDED_CHANNELS = "Suspended-Channels";

    public static final String IGNORED_CHANNELS = "Ignored-Channels";

}