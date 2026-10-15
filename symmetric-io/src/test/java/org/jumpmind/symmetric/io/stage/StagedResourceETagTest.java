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
package org.jumpmind.symmetric.io.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

class StagedResourceETagTest {
    @Test
    void testToJsonAndFromJson_roundTrips() {
        StagedResourceETag etag = new StagedResourceETag(123456789L, 987654321L);
        StagedResourceETag roundTripped = StagedResourceETag.fromJson(etag.toJson());
        assertEquals(etag, roundTripped);
        assertEquals(etag.getGenerationTime(), roundTripped.getGenerationTime());
        assertEquals(etag.getSize(), roundTripped.getSize());
        assertEquals(StagedResourceETag.CURRENT_VERSION, roundTripped.getVersion());
    }

    @Test
    void testToHeaderValue_isQuotedOpaqueString() {
        String headerValue = new StagedResourceETag(123L, 456L).toHeaderValue();
        String quotedContent = headerValue.substring(1, headerValue.length() - 1);
        assertEquals('"', headerValue.charAt(0));
        assertEquals('"', headerValue.charAt(headerValue.length() - 1));
        assertEquals(-1, quotedContent.indexOf('"'));
    }

    @Test
    void testToHeaderValueAndFromHeaderValue_roundTrips() {
        StagedResourceETag etag = new StagedResourceETag(123L, 456L);
        assertEquals(etag, StagedResourceETag.fromHeaderValue(etag.toHeaderValue()));
    }

    @Test
    void testFromHeaderValue_withNull_returnsNull() {
        assertNull(StagedResourceETag.fromHeaderValue(null));
    }

    @Test
    void testFromHeaderValue_withUnquotedValue_returnsNull() {
        assertNull(StagedResourceETag.fromHeaderValue(new StagedResourceETag(123L, 456L).toJson()));
    }

    @Test
    void testFromHeaderValue_withWeakValidator_returnsNull() {
        assertNull(StagedResourceETag.fromHeaderValue("W/" + new StagedResourceETag(123L, 456L).toHeaderValue()));
    }

    @Test
    void testFromHeaderValue_withInvalidBase64_returnsNull() {
        assertNull(StagedResourceETag.fromHeaderValue("\"not base64!\""));
    }

    @Test
    void testFromHeaderValue_withHttpDate_returnsNull() {
        assertNull(StagedResourceETag.fromHeaderValue("Wed, 21 Oct 2026 07:28:00 GMT"));
    }

    @Test
    void testFromJson_withNull_returnsNull() {
        assertNull(StagedResourceETag.fromJson(null));
    }

    @Test
    void testFromJson_withEmptyString_returnsNull() {
        assertNull(StagedResourceETag.fromJson(""));
    }

    @Test
    void testFromJson_withMalformedJson_returnsNull() {
        assertNull(StagedResourceETag.fromJson("{not valid json"));
    }

    @Test
    void testFromJson_withFutureVersion_returnsNull() {
        String json = "{\"version\":" + (StagedResourceETag.CURRENT_VERSION + 1) + ",\"generationTime\":1,\"size\":2}";
        assertNull(StagedResourceETag.fromJson(json));
    }

    @Test
    void testEquals_differsWhenGenerationTimeOrSizeDiffer() {
        StagedResourceETag base = new StagedResourceETag(100L, 200L);
        StagedResourceETag differentGenerationTime = new StagedResourceETag(101L, 200L);
        StagedResourceETag differentSize = new StagedResourceETag(100L, 201L);
        assertEquals(base, new StagedResourceETag(100L, 200L));
        assertNotEquals(base, differentGenerationTime);
        assertNotEquals(base, differentSize);
    }
}
