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
package org.jumpmind.symmetric.route;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

import org.jumpmind.symmetric.common.Constants;

/**
 * Accumulates gap detection figures across routing cycles and describes them for the periodic summary log message.
 */
public class DataGapDetectionSummary {
    private static final long NO_DATA_ID = -1;
    private static final long NO_FULL_ANALYSIS = 0;
    private static final DateTimeFormatter TIME_FORMAT = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    private long lastSummaryLogTime;
    private int cycleCount;
    private long totalDetectionTimeInMs;
    private long maxDetectionTimeInMs;
    private long lastDetectionTimeInMs;
    private long gapsAdded;
    private long gapsDeleted;
    private int openGapCount;
    private long lastDataId = NO_DATA_ID;
    private long lastFullAnalysisTime = NO_FULL_ANALYSIS;

    public DataGapDetectionSummary(long creationTime) {
        this.lastSummaryLogTime = creationTime;
    }

    public synchronized void recordDetection(long detectionTimeInMs, int openGapCount) {
        cycleCount++;
        totalDetectionTimeInMs += detectionTimeInMs;
        maxDetectionTimeInMs = Math.max(maxDetectionTimeInMs, detectionTimeInMs);
        lastDetectionTimeInMs = detectionTimeInMs;
        this.openGapCount = openGapCount;
    }

    public synchronized void recordGapChanges(int addedCount, int deletedCount) {
        gapsAdded += addedCount;
        gapsDeleted += deletedCount;
    }

    public synchronized void recordLastDataId(long dataId) {
        lastDataId = dataId;
    }

    public synchronized void recordFullAnalysis(long analysisTime) {
        lastFullAnalysisTime = analysisTime;
    }

    public synchronized String getInfoMessage(long currentTime) {
        long elapsedTimeInMs = currentTime - lastSummaryLogTime;
        long averageDetectionTimeInMs = cycleCount == 0 ? 0 : totalDetectionTimeInMs / cycleCount;
        return String.format("Gap detection summary for the last %d ms: %d cycles, average detection time %d ms, maximum detection time %d ms, "
                + "%d open gaps, %d gaps added, %d gaps deleted, last data ID %s, last full gap analysis %s",
                elapsedTimeInMs, cycleCount, averageDetectionTimeInMs, maxDetectionTimeInMs, openGapCount, gapsAdded, gapsDeleted,
                describeLastDataId(), describeLastFullAnalysis());
    }

    public synchronized String getDebugMessage() {
        return String.format("Gap detection took %s, %d gaps added and %d gaps deleted since the last summary",
                describeDetectionTime(lastDetectionTimeInMs), gapsAdded, gapsDeleted);
    }

    public synchronized long getLastSummaryLogTime() {
        return lastSummaryLogTime;
    }

    public synchronized void setLastSummaryLogTime(long lastLogTime) {
        lastSummaryLogTime = lastLogTime;
    }

    public synchronized void resetCounters() {
        cycleCount = 0;
        totalDetectionTimeInMs = 0;
        maxDetectionTimeInMs = 0;
        gapsAdded = 0;
        gapsDeleted = 0;
    }

    synchronized int getCycleCount() {
        return cycleCount;
    }

    synchronized long getGapsAdded() {
        return gapsAdded;
    }

    synchronized long getGapsDeleted() {
        return gapsDeleted;
    }

    synchronized int getOpenGapCount() {
        return openGapCount;
    }

    synchronized long getLastDataId() {
        return lastDataId;
    }

    synchronized long getLastFullAnalysisTime() {
        return lastFullAnalysisTime;
    }

    private String describeDetectionTime(long detectionTimeInMs) {
        if (detectionTimeInMs > Constants.LONG_OPERATION_DEBUG_THRESHOLD) {
            return String.format(Locale.ROOT, "%.1f seconds", detectionTimeInMs / 1000.0);
        }
        return detectionTimeInMs + " ms";
    }

    private String describeLastDataId() {
        return lastDataId == NO_DATA_ID ? "none" : String.valueOf(lastDataId);
    }

    private String describeLastFullAnalysis() {
        if (lastFullAnalysisTime == NO_FULL_ANALYSIS) {
            return "none";
        }
        Instant lastFullAnalysis = Instant.ofEpochMilli(lastFullAnalysisTime);
        return TIME_FORMAT.format(LocalDateTime.ofInstant(lastFullAnalysis, ZoneId.systemDefault()));
    }
}
