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
package org.jumpmind.symmetric.service;

/**
 * Names for jobs as locked by the {@link IClusterService} 
 */
public class ClusterConstants {
    
    public static final String STAGE_MANAGEMENT = "STAGE_MANAGEMENT";
    public static final String ROUTE = "ROUTE";
    public static final String PUSH = "PUSH";
    public static final String PULL = "PULL";
    public static final String OFFLINE_PUSH = "OFFLINE_PUSH";
    public static final String OFFLINE_PULL = "OFFLINE_PULL";
    public static final String REFRESH_CACHE = "REFRESH_CACHE";
    public static final String PURGE_OUTGOING = "PURGE_OUTGOING";
    public static final String PURGE_INCOMING = "PURGE_INCOMING";
    public static final String PURGE_STATISTICS = "PURGE_STATISTICS";
    public static final String PURGE_DATA_GAPS = "PURGE_DATA_GAPS";
    public static final String HEARTBEAT = "HEARTBEAT";
    public static final String INITIAL_LOAD_EXTRACT = "INITIAL_LOAD_EXTRACT";
    public static final String SYNCTRIGGERS = "SYNCTRIGGERS";
    public static final String WATCHDOG = "WATCHDOG";
    public static final String STATISTICS = "STATISTICS";
    public static final String FILE_SYNC_TRACKER = "FILE_SYNC_TRACKER";
    public static final String FILE_SYNC_SCAN = "FILE_SYNC_SCAN";
    public static final String FILE_SYNC_SHARED = "FILE_SYNC_SHARED";
    public static final String FILE_SYNC_PULL = "FILE_SYNC_PULL";
    public static final String FILE_SYNC_PUSH = "FILE_SYNC_PUSH";

    public static final String TYPE_CLUSTER = "CLUSTER";
    public static final String TYPE_EXCLUSIVE = "EXCLUSIVE";
    public static final String TYPE_SHARED = "SHARED";

}