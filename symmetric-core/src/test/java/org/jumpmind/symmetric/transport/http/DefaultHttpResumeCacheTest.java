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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.io.stage.StagedResourceETag;
import org.jumpmind.symmetric.service.IParameterService;
import org.junit.jupiter.api.Test;

class DefaultHttpResumeCacheTest {
    private static final long TIME_TO_LIVE_MS = 3_600_000L;

    @Test
    void testPutAndGet_returnsStoredEntry() {
        DefaultHttpResumeCache cache = newCache();
        ResumeCacheEntry entry = createEntry("node1", 100L);
        cache.put("node1", 100L, entry);
        assertEquals(entry, cache.get("node1", 100L));
    }

    @Test
    void testGet_withWrongBatchId_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        assertNull(cache.get("node1", 101L));
    }

    @Test
    void testGet_withWrongNodeId_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        assertNull(cache.get("node2", 100L));
    }

    @Test
    void testGet_whenEmpty_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        assertNull(cache.get("node1", 100L));
    }

    @Test
    void testGet_withEntryOlderThanTimeToLive_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createExpiredEntry("node1", 100L, false));
        assertNull(cache.get("node1", 100L));
    }

    @Test
    void testGet_withNonPositiveTimeToLive_neverExpires() {
        DefaultHttpResumeCache cache = new DefaultHttpResumeCache(engineWithTimeToLive(0L));
        ResumeCacheEntry entry = createExpiredEntry("node1", 100L, false);
        cache.put("node1", 100L, entry);
        assertEquals(entry, cache.get("node1", 100L));
    }

    @Test
    void testGetPendingForNode_returnsEntryForMatchingNode() {
        DefaultHttpResumeCache cache = newCache();
        ResumeCacheEntry entry = createEntry("node1", 100L);
        cache.put("node1", 100L, entry);
        assertEquals(entry, cache.getPendingForNode("node1", Constants.QUEUE_DEFAULT));
    }

    @Test
    void testGetPendingForNode_withNoMatch_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        assertNull(cache.getPendingForNode("node2", Constants.QUEUE_DEFAULT));
    }

    @Test
    void testGetPendingForNode_withMismatchedQueue_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        assertNull(cache.getPendingForNode("node1", Constants.QUEUE_SYSTEM));
    }

    @Test
    void testGetPendingForNode_withEntryOlderThanTimeToLive_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createExpiredEntry("node1", 100L, false));
        assertNull(cache.getPendingForNode("node1", Constants.QUEUE_DEFAULT));
    }

    @Test
    void testGetPendingFileSyncEntryForNode_returnsEntryIgnoringQueue() {
        DefaultHttpResumeCache cache = newCache();
        ResumeCacheEntry entry = createFileSyncEntry("node1", 100L, Constants.QUEUE_RELOAD);
        cache.put("node1", 100L, entry);
        assertEquals(entry, cache.getPendingFileSyncEntryForNode("node1"));
    }

    @Test
    void testGetPendingFileSyncEntryForNode_withTableSyncEntry_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        assertNull(cache.getPendingFileSyncEntryForNode("node1"));
    }

    @Test
    void testGetPendingFileSyncEntryForNode_withWrongNodeId_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createFileSyncEntry("node1", 100L, Constants.QUEUE_DEFAULT));
        assertNull(cache.getPendingFileSyncEntryForNode("node2"));
    }

    @Test
    void testGetPendingFileSyncEntryForNode_withEntryOlderThanTimeToLive_returnsNull() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createExpiredEntry("node1", 100L, true));
        assertNull(cache.getPendingFileSyncEntryForNode("node1"));
    }

    @Test
    void testRemove_withMatchingKey_clearsEntry() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        cache.remove("node1", 100L);
        assertNull(cache.get("node1", 100L));
        assertNull(cache.getPendingForNode("node1", Constants.QUEUE_DEFAULT));
    }

    @Test
    void testRemove_withNonMatchingKey_leavesEntryInPlace() {
        DefaultHttpResumeCache cache = newCache();
        ResumeCacheEntry entry = createEntry("node1", 100L);
        cache.put("node1", 100L, entry);
        cache.remove("node1", 999L);
        assertEquals(entry, cache.get("node1", 100L));
    }

    @Test
    void testPut_differentOwnerWhileSlotBusy_doesNotEvictExistingEntry() {
        DefaultHttpResumeCache cache = newCache();
        ResumeCacheEntry first = createEntry("node1", 100L);
        cache.put("node1", 100L, first);
        cache.put("node2", 200L, createEntry("node2", 200L));
        assertEquals(first, cache.get("node1", 100L));
        assertNull(cache.get("node2", 200L));
    }

    @Test
    void testPut_sameOwnerReRegistering_replacesEntry() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        ResumeCacheEntry updated = createEntry("node1", 100L);
        cache.put("node1", 100L, updated);
        assertEquals(updated, cache.get("node1", 100L));
    }

    @Test
    void testPut_afterRemove_slotAcceptsNewOwner() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createEntry("node1", 100L));
        cache.remove("node1", 100L);
        ResumeCacheEntry second = createEntry("node2", 200L);
        cache.put("node2", 200L, second);
        assertEquals(second, cache.get("node2", 200L));
    }

    @Test
    void testPut_whenSlotHoldsExpiredEntry_storesNewEntry() {
        DefaultHttpResumeCache cache = newCache();
        cache.put("node1", 100L, createExpiredEntry("node1", 100L, false));
        ResumeCacheEntry replacement = createEntry("node2", 200L);
        cache.put("node2", 200L, replacement);
        assertEquals(replacement, cache.get("node2", 200L));
    }

    private DefaultHttpResumeCache newCache() {
        return new DefaultHttpResumeCache(engineWithTimeToLive(TIME_TO_LIVE_MS));
    }

    private ISymmetricEngine engineWithTimeToLive(long timeToLiveMs) {
        ISymmetricEngine engine = mock(ISymmetricEngine.class);
        IParameterService parameterService = mock(IParameterService.class);
        when(engine.getParameterService()).thenReturn(parameterService);
        when(parameterService.getLong(ParameterConstants.STREAM_TO_FILE_TIME_TO_LIVE_MS)).thenReturn(timeToLiveMs);
        return engine;
    }

    private ResumeCacheEntry createExpiredEntry(String nodeId, long batchId, boolean isFileSync) {
        return ResumeCacheEntry.builder()
                .nodeId(nodeId)
                .batchId(batchId)
                .etag(new StagedResourceETag(1L, 2L))
                .receivedCount(3L)
                .cachedAtTime(System.currentTimeMillis() - 2 * TIME_TO_LIVE_MS)
                .queue(Constants.QUEUE_DEFAULT)
                .fileSync(isFileSync)
                .build();
    }

    private ResumeCacheEntry createEntry(String nodeId, long batchId) {
        return ResumeCacheEntry.builder()
                .nodeId(nodeId)
                .batchId(batchId)
                .etag(new StagedResourceETag(1L, 2L))
                .receivedCount(3L)
                .channelId("channel1")
                .binaryEncoding("NONE")
                .cachedAtTime(System.currentTimeMillis())
                .queue(Constants.QUEUE_DEFAULT)
                .build();
    }

    private ResumeCacheEntry createFileSyncEntry(String nodeId, long batchId, String queue) {
        return ResumeCacheEntry.builder()
                .nodeId(nodeId)
                .batchId(batchId)
                .etag(new StagedResourceETag(1L, 2L))
                .receivedCount(3L)
                .cachedAtTime(System.currentTimeMillis())
                .queue(queue)
                .fileSync(true)
                .build();
    }
}
