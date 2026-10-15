/**
 * Licensed to JumpMind Inc under one or more contributor
 * license agreements.  See the NOTICE file distributed
 * with this work for additional information regarding
 * copyright ownership.  JumpMind Inc licenses this file
 * to you under the GNU Affero General Public License, version 3.0 (AGPLv3)
 * (the "License"); you may not use this file except in compliance
 * with the License.
 *
 * You should have received a copy of the GNU Affero General Public License,
 * version 3.0 (AGPLv3) along with this library; if not, see
 * <http://www.gnu.org/licenses/>.
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.jumpmind.symmetric.transport.http;

import java.util.Objects;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Open-source default {@link IHttpResumeCache}: a single-slot holder, since a base engine only ever has one batch in flight at a time per (node, queue) pair
 * and only the one batch that was in flight when a connection dropped is ever a resume candidate. An entry older than the staging time-to-live
 * ({@code stream.to.file.ttl.ms}, where zero or less means entries never expire) is evicted, since the peer no longer has the staged batch to resume from by
 * then. A Pro implementation may back this with a bounded, multi-entry map instead.
 */
public class DefaultHttpResumeCache implements IHttpResumeCache {
    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ISymmetricEngine engine;
    private ResumeCacheEntry entry;

    /**
     * Matches the {@code (ISymmetricEngine)} constructor shape {@code AppUtils.newInstance} requires; the engine supplies the time-to-live for entries.
     */
    public DefaultHttpResumeCache(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public synchronized void put(String nodeId, long batchId, ResumeCacheEntry newEntry) {
        evictExpiredEntry();
        if (entry == null || matches(nodeId, batchId)) {
            entry = newEntry;
        } else {
            log.debug("Resume cache slot busy with node {} batch {}; not registering node {} batch {}",
                    entry.getNodeId(), entry.getBatchId(), nodeId, batchId);
        }
    }

    @Override
    public synchronized ResumeCacheEntry get(String nodeId, long batchId) {
        evictExpiredEntry();
        if (matches(nodeId, batchId)) {
            return entry;
        }
        return null;
    }

    @Override
    public synchronized ResumeCacheEntry getPendingForNode(String nodeId, String queue) {
        evictExpiredEntry();
        if (entry != null && entry.getNodeId().equals(nodeId) && Objects.equals(entry.getQueue(), queue)) {
            return entry;
        }
        return null;
    }

    @Override
    public synchronized ResumeCacheEntry getPendingFileSyncEntryForNode(String nodeId) {
        evictExpiredEntry();
        if (entry != null && entry.getNodeId().equals(nodeId) && entry.isFileSync()) {
            return entry;
        }
        return null;
    }

    @Override
    public synchronized void remove(String nodeId, long batchId) {
        if (matches(nodeId, batchId)) {
            entry = null;
        }
    }

    private void evictExpiredEntry() {
        if (entry == null) {
            return;
        }
        long timeToLiveMs = engine.getParameterService().getLong(ParameterConstants.STREAM_TO_FILE_TIME_TO_LIVE_MS);
        if (timeToLiveMs > 0 && System.currentTimeMillis() - entry.getCachedAtTime() > timeToLiveMs) {
            log.debug("Evicting expired resume entry for node {} batch {}", entry.getNodeId(), entry.getBatchId());
            entry = null;
        }
    }

    private boolean matches(String nodeId, long batchId) {
        return entry != null && entry.getBatchId() == batchId && entry.getNodeId().equals(nodeId);
    }
}
