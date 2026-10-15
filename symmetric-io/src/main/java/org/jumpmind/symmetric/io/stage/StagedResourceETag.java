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

import java.nio.charset.StandardCharsets;
import java.util.Base64;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;

/**
 * Identifies a specific version of a staged resource's content, so a resumed transfer can tell whether a partially-received copy is still valid to append to,
 * or stale and needing a full resend. Based on the resource's stable {@link IStagedResource#getGenerationTime()}, not
 * {@link IStagedResource#getLastUpdateTime()}, which is refreshed on every access and would make every ETag comparison a false mismatch.
 */
public class StagedResourceETag {
    public static final int CURRENT_VERSION = 1;
    private static final Logger log = LoggerFactory.getLogger(StagedResourceETag.class);
    private static final Gson GSON = new Gson();
    private int version = CURRENT_VERSION;
    private long generationTime;
    private long size;

    public StagedResourceETag() {
    }

    public StagedResourceETag(long generationTime, long size) {
        this.generationTime = generationTime;
        this.size = size;
    }

    public int getVersion() {
        return version;
    }

    public void setVersion(int version) {
        this.version = version;
    }

    public long getGenerationTime() {
        return generationTime;
    }

    public void setGenerationTime(long generationTime) {
        this.generationTime = generationTime;
    }

    public long getSize() {
        return size;
    }

    public void setSize(long size) {
        this.size = size;
    }

    public String toJson() {
        return GSON.toJson(this);
    }

    /**
     * An entity-tag must be an opaque quoted string (RFC 9110 section 8.8.3). Base64-encoding the JSON first guarantees the payload can never contain a quote
     * or other syntax-breaking character, so the same value serves as the {@code ETag} response header and as the {@code If-Range} request header.
     */
    public String toHeaderValue() {
        return "\"" + Base64.getEncoder().encodeToString(toJson().getBytes(StandardCharsets.UTF_8)) + "\"";
    }

    /**
     * Never throws. A missing, weak, or otherwise unparseable {@code If-Range} value (including an HTTP-date) means "no match", so the caller does a full
     * resend rather than failing the request.
     *
     * @return the parsed ETag, or {@code null} when {@code headerValue} is not a value produced by {@link #toHeaderValue()}
     */
    public static StagedResourceETag fromHeaderValue(String headerValue) {
        if (headerValue == null) {
            return null;
        }
        String trimmed = headerValue.trim();
        if (trimmed.length() < 2 || !trimmed.startsWith("\"") || !trimmed.endsWith("\"")) {
            return null;
        }
        try {
            byte[] json = Base64.getDecoder().decode(trimmed.substring(1, trimmed.length() - 1));
            return fromJson(new String(json, StandardCharsets.UTF_8));
        } catch (IllegalArgumentException e) {
            log.debug("Ignoring staged resource ETag header value that is not valid Base64: " + headerValue, e);
            return null;
        }
    }

    /**
     * Never throws. A stale, malformed, or future-incompatible value should be treated as "no match, do a full resend" rather than fail the request.
     */
    public static StagedResourceETag fromJson(String json) {
        if (json == null || json.isEmpty()) {
            return null;
        }
        try {
            StagedResourceETag etag = GSON.fromJson(json, StagedResourceETag.class);
            if (etag == null || etag.version != CURRENT_VERSION) {
                return null;
            }
            return etag;
        } catch (JsonSyntaxException e) {
            log.debug("Ignoring unparseable staged resource ETag: {}", json, e);
            return null;
        }
    }

    @Override
    public int hashCode() {
        return Long.hashCode(version * 31L + generationTime * 31L + size);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (!(obj instanceof StagedResourceETag)) {
            return false;
        }
        StagedResourceETag other = (StagedResourceETag) obj;
        return version == other.version && generationTime == other.generationTime && size == other.size;
    }

    @Override
    public String toString() {
        return toJson();
    }
}
