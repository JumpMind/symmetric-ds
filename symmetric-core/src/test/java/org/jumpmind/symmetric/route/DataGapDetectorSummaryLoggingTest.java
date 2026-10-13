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
package org.jumpmind.symmetric.route;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import org.jumpmind.symmetric.common.Constants;
import org.jumpmind.symmetric.db.ISymmetricDialect;
import org.jumpmind.symmetric.model.DataGap;
import org.jumpmind.symmetric.service.IClusterService;
import org.jumpmind.symmetric.service.IContextService;
import org.jumpmind.symmetric.service.IDataService;
import org.jumpmind.symmetric.service.INodeService;
import org.jumpmind.symmetric.service.IParameterService;
import org.jumpmind.symmetric.service.IRouterService;
import org.jumpmind.symmetric.statistic.IStatisticManager;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.Logger;

class DataGapDetectorSummaryLoggingTest {
    Logger detectorLogger;
    Level originalLevel;
    ListLogAppender appender;
    long startTime;
    long debugDueTime;
    long infoDueTime;
    DataGapPassStats passStats;
    DataGapFastDetector detector;

    @BeforeEach
    void setUp() {
        detectorLogger = (Logger) LogManager.getLogger(DataGapFastDetector.class);
        originalLevel = detectorLogger.getLevel();
        detectorLogger.setLevel(Level.DEBUG);
        appender = new ListLogAppender();
        appender.start();
        detectorLogger.addAppender(appender);
        startTime = System.currentTimeMillis();
        debugDueTime = startTime + Constants.LONG_OPERATION_DEBUG_THRESHOLD + 1000;
        infoDueTime = startTime + Constants.LONG_OPERATION_THRESHOLD + 1000;
        passStats = new DataGapPassStats(startTime);
        detector = new DataGapFastDetector(mock(IDataService.class), mock(IParameterService.class), mock(IContextService.class),
                mock(ISymmetricDialect.class), mock(IRouterService.class), mock(IStatisticManager.class), mock(INodeService.class),
                mock(IClusterService.class));
        detector.getDetectionSummary().recordDetection(100, 3);
    }

    @AfterEach
    void tearDown() {
        detectorLogger.removeAppender(appender);
        detectorLogger.setLevel(originalLevel);
    }

    @Test
    void testSummaryNothingLoggedBeforeDebugThreshold() {
        detector.logGapDetectionSummary(startTime, passStats);
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testSummaryDebugLoggedForShortPassAfterDebugThreshold() {
        detector.logGapDetectionSummary(debugDueTime, new DataGapPassStats(debugDueTime - 100));
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.DEBUG, events.get(0).getLevel());
        assertEquals("Gap detection took 100 ms, 0 gaps added and 0 gaps deleted since the last summary", events.get(0).getMessage().getFormattedMessage());
    }

    @Test
    void testSummaryInfoLoggedForLongPassAfterInfoThreshold() {
        detector.logGapDetectionSummary(infoDueTime, passStats);
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.INFO, events.get(0).getLevel());
        assertEquals(true, events.get(0).getMessage().getFormattedMessage().startsWith("Gap detection summary for the last "));
    }

    @Test
    void testSummaryLongPassBeforeInfoThresholdLogsNothing() {
        detector.logGapDetectionSummary(debugDueTime, passStats);
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testSummaryInfoKeepsCounters() {
        detector.logGapDetectionSummary(infoDueTime, passStats);
        assertEquals(1, detector.getDetectionSummary().getCycleCount());
    }

    @Test
    void testSummaryDebugKeepsCounters() {
        detector.logGapDetectionSummary(debugDueTime, new DataGapPassStats(debugDueTime - 100));
        assertEquals(1, detector.getDetectionSummary().getCycleCount());
    }

    @Test
    void testSummaryInfoUpdatesLastSummaryLogTime() {
        detector.logGapDetectionSummary(infoDueTime, passStats);
        assertEquals(infoDueTime, detector.getDetectionSummary().getLastSummaryLogTime());
    }

    @Test
    void testSummaryDebugKeepsLastSummaryLogTime() {
        long lastSummaryLogTime = detector.getDetectionSummary().getLastSummaryLogTime();
        detector.logGapDetectionSummary(debugDueTime, new DataGapPassStats(debugDueTime - 100));
        assertEquals(lastSummaryLogTime, detector.getDetectionSummary().getLastSummaryLogTime());
    }

    @Test
    void testSummaryNothingLoggedRightAfterInfo() {
        detector.logGapDetectionSummary(infoDueTime, passStats);
        detector.logGapDetectionSummary(infoDueTime + 1, passStats);
        assertEquals(1, appender.getEvents().size());
    }

    @Test
    void testSummaryWithoutPassStatsLogsNothing() {
        detector.logGapDetectionSummary(infoDueTime, null);
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testProgressNothingLoggedBeforeDebugThreshold() {
        detector.logGapDetectionProgress(startTime, populatedPassStats());
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testProgressDebugLoggedAfterDebugThreshold() {
        detector.logGapDetectionProgress(debugDueTime, populatedPassStats());
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.DEBUG, events.get(0).getLevel());
        assertEquals("Gap detection running for 6000 ms: 7 routed rows found in gap range 100, 2 gaps added, 1 gaps deleted, 3 queries",
                events.get(0).getMessage().getFormattedMessage());
    }

    @Test
    void testProgressInfoLoggedAfterInfoThreshold() {
        detector.logGapDetectionProgress(infoDueTime, populatedPassStats());
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.INFO, events.get(0).getLevel());
        assertEquals("Gap detection running for 31000 ms: 7 routed rows found in gap range 100, 2 gaps added, 1 gaps deleted, 3 queries",
                events.get(0).getMessage().getFormattedMessage());
    }

    @Test
    void testProgressWithoutPassStatsLogsNothing() {
        detector.logGapDetectionProgress(infoDueTime, null);
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testFullProgressNothingLoggedBeforeDebugThreshold() {
        useGapChanges();
        detector.logFullGapDetectionProgress(startTime, populatedPassStats());
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testFullProgressDebugLoggedAfterDebugThreshold() {
        useGapChanges();
        detector.logFullGapDetectionProgress(debugDueTime, populatedPassStats());
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.DEBUG, events.get(0).getLevel());
        assertEquals("Full gap detection running for 6000 ms: 7 routed rows found in gap range 100, 3 gaps added, 2 gaps deleted, "
                + "data checked in 4 gaps, 3 queries", events.get(0).getMessage().getFormattedMessage());
    }

    @Test
    void testFullProgressInfoLoggedAfterInfoThreshold() {
        useGapChanges();
        detector.logFullGapDetectionProgress(infoDueTime, populatedPassStats());
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.INFO, events.get(0).getLevel());
        assertEquals("Full gap detection running for 31000 ms: 7 routed rows found in gap range 100, 3 gaps added, 2 gaps deleted, "
                + "data checked in 4 gaps, 3 queries", events.get(0).getMessage().getFormattedMessage());
    }

    @Test
    void testGapMappingTimeNothingLoggedBeforeDebugThreshold() {
        useGapsAndDataIds();
        detector.logGapMappingTime(startTime, passStats);
        assertEquals(0, appender.getEvents().size());
    }

    @Test
    void testGapMappingTimeDebugLoggedAfterDebugThreshold() {
        useGapsAndDataIds();
        detector.logGapMappingTime(debugDueTime, passStats);
        List<LogEvent> events = appender.getEvents();
        assertEquals(1, events.size());
        assertEquals(Level.DEBUG, events.get(0).getLevel());
        assertEquals("It took 6000ms to map 3 data IDs into 2 gaps", events.get(0).getMessage().getFormattedMessage());
    }

    @Test
    void testGapMappingTimeWithoutPassStatsLogsNothing() {
        useGapsAndDataIds();
        detector.logGapMappingTime(infoDueTime, null);
        assertEquals(0, appender.getEvents().size());
    }

    private DataGapPassStats populatedPassStats() {
        DataGapPassStats stats = new DataGapPassStats(startTime);
        stats.addIdsFound(7);
        stats.addRangeChecked(100);
        stats.incrementGapsAdded();
        stats.incrementGapsAdded();
        stats.incrementGapsDeleted();
        for (int i = 0; i < 4; i++) {
            stats.incrementGapsExpireChecked();
        }
        for (int i = 0; i < 3; i++) {
            stats.incrementGapQueriesCount();
        }
        return stats;
    }

    private void useGapChanges() {
        detector.gapsAdded = new HashSet<DataGap>();
        detector.gapsAdded.add(new DataGap(1, 1));
        detector.gapsAdded.add(new DataGap(2, 2));
        detector.gapsAdded.add(new DataGap(3, 3));
        detector.gapsDeleted = new HashSet<DataGap>();
        detector.gapsDeleted.add(new DataGap(4, 4));
        detector.gapsDeleted.add(new DataGap(5, 5));
    }

    private void useGapsAndDataIds() {
        detector.gaps = new ArrayList<DataGap>();
        detector.gaps.add(new DataGap(1, 1));
        detector.gaps.add(new DataGap(2, 2));
        detector.dataIds = new ArrayList<Long>();
        detector.dataIds.add(10L);
        detector.dataIds.add(11L);
        detector.dataIds.add(12L);
    }
}
