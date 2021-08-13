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
    public static final String STAGE_MANAGEMENT = "Stage Management";
    public static final String ROUTE = "Routing";
    public static final String PUSH = "Push";
    public static final String PULL = "Pull";
    public static final String OFFLINE_PUSH = "Offline Push";
    public static final String OFFLINE_PULL = "Offline Pull";
    public static final String REFRESH_CACHE = "Refresh Cache";
    public static final String PURGE_OUTGOING = "Purge Outgoing";
    public static final String PURGE_INCOMING = "Purge Incoming";
    public static final String PURGE_STATISTICS = "Purge Statistics";
    public static final String REPORT_STATUS = "Report Status";
    public static final String PURGE_DATA_GAPS = "Purge Data Gaps";
    public static final String HEARTBEAT = "Heartbeat";
    public static final String INITIAL_LOAD_EXTRACT = "Initial Load Extract";
    public static final String INITIAL_LOAD_QUEUE = "Initial Load Queue";
    public static final String SYNC_CONFIG = "Sync Config";
    public static final String SYNC_TRIGGERS = "SyncTriggers";
    public static final String WATCHDOG = "Watchdog";
    public static final String STATISTICS = "Stat Flush";
    public static final String FILE_SYNC_TRACKER = "File Sync Tracker";
    public static final String FILE_SYNC_PULL = "File Sync Pull";
    public static final String FILE_SYNC_PUSH = "File Sync Push";
    public static final String MONITOR = "Monitor";
    public static final String LOG_MINER = "Log Miner";
    public static final String FILE_SYNC_SHARED = "FILE_SYNC_SHARED";
    public static final String TYPE_CLUSTER = "CLUSTER";
    public static final String TYPE_EXCLUSIVE = "EXCLUSIVE";
    public static final String TYPE_SHARED = "SHARED";
}