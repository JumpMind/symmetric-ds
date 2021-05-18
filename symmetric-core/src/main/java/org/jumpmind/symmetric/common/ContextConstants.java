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
package org.jumpmind.symmetric.common;

final public class ContextConstants {

    private ContextConstants() {
    }
    
    public static final String ROUTING_FULL_GAP_ANALYSIS = "routing.full.gap.analysis";
    
    public static final String ROUTING_LAST_BUSY_EXPIRE_RUN_TIME = "routing.last.busy.expire.run.time";
    
    public static final String GUID = "guid";

    public static final String FILE_SYNC_FAST_SCAN_TRACK_TIME = "file.sync.fast.scan.track.time";

    public static final String CONTEXT_BULK_WRITER_TO_USE = "bulkWriterToUse";
    
    public static final String PURGE_LAST_DATA_ID = "purge.last.data.id";
    
    public static final String PURGE_LAST_EVENT_BATCH_ID = "purge.last.event.batch.id";
    
    public static final String PURGE_LAST_BATCH_ID = "purge.last.batch.id";
    
    public static final String LOG_MINER_NEXT_ID = "log.miner.next.id";
    
    public static final String LOG_MINER_OPEN_TRANSACTIONS = "log.miner.open.transactions";
    
}
