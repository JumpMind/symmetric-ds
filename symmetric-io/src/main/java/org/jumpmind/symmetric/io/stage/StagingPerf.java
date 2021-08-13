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
package org.jumpmind.symmetric.io.stage;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.RandomStringUtils;
import org.jumpmind.db.util.BinaryEncoding;
import org.jumpmind.symmetric.io.data.Batch;
import org.jumpmind.symmetric.io.data.Batch.BatchType;
import org.jumpmind.symmetric.io.stage.IStagedResource.State;
import org.jumpmind.util.AppUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StagingPerf {
    protected final static String STAGE_PATH = "test";
    protected final static String STAT_LOCK_ACQUIRE = "Acquire Lock";
    protected final static String STAT_BATCH_CREATE = "Create Batch File";
    protected final static String STAT_BATCH_WRITE = "Write Batch File";
    protected final static String STAT_BATCH_RENAME = "Rename Batch File";
    protected final static String STAT_BATCH_FIND = "Find Batch File";
    protected final static String STAT_BATCH_READ = "Read Batch File";
    protected Logger log = LoggerFactory.getLogger(getClass());
    protected IStagingManager stagingMgr;
    protected StagingPerfListener listener;
    protected String serverInfo;

    public StagingPerf(IStagingManager stagingMgr, StagingPerfListener listener) {
        this.stagingMgr = stagingMgr;
        this.listener = listener;
        serverInfo = String.format("Server: '%s' Host: '%s' IP: '%s'", getClass().getName(), AppUtils.getHostName(), AppUtils.getIpAddress());
    }

    public List<StagingPerfResult> run(int seconds) {
        Map<String, StagingPerfResult> results = new HashMap<String, StagingPerfResult>();
        long startTime = System.currentTimeMillis();
        long lastCallbackTime = startTime;
        long totalSeconds = 0;
        log.info("Starting staging test, duration of {} seconds", seconds);
        try {
            SecureRandom random = new SecureRandom();
            long startBatchId = random.nextInt(999999) + 1;
            long endBatchId = startBatchId + (seconds * 500);
            for (long batchId = startBatchId; batchId < endBatchId; batchId++) {
                Batch batch = new Batch(BatchType.EXTRACT, 0, "default", BinaryEncoding.HEX, "master", "1", true);
                batch.setBatchId(1);
                testBatch(batch, results);
                if (Thread.interrupted()) {
                    log.warn("Test ending because thread interrupted");
                    break;
                }
                long time = System.currentTimeMillis();
                totalSeconds = ((time - startTime) / 1000);
                if (totalSeconds >= seconds) {
                    break;
                }
                if (time - lastCallbackTime > 1000) {
                    List<StagingPerfResult> resultsAsList = getResultsAsList(results);
                    logResults(totalSeconds, resultsAsList);
                    listener.update(getResultsAsList(results), (totalSeconds / (float) seconds));
                    lastCallbackTime = time;
                }
            }
        } catch (Exception e) {
            log.error("Failed to run test", e);
        }
        List<StagingPerfResult> resultsAsList = getResultsAsList(results);
        logResults(totalSeconds, resultsAsList);
        return resultsAsList;
    }

    protected void logResults(long totalSeconds, List<StagingPerfResult> resultsAsList) {
        log.info("Running for {} seconds", totalSeconds);
        for (StagingPerfResult result : resultsAsList) {
            log.info(result.toString());
        }
    }

    protected void testBatch(Batch batch, Map<String, StagingPerfResult> results) {
        long ts = System.currentTimeMillis();
        StagingFileLock lock = stagingMgr.acquireFileLock(serverInfo, STAGE_PATH, batch.getStagedLocation(), batch.getBatchId());
        if (lock.isAcquired()) {
            increment(results, STAT_LOCK_ACQUIRE, System.currentTimeMillis() - ts);
            lock.releaseLock();
        } else {
            throw new RuntimeException("Failed to create lock file");
        }
        ts = System.currentTimeMillis();
        IStagedResource resource = stagingMgr.create(STAGE_PATH, batch.getStagedLocation(), batch.getBatchId());
        if (resource != null) {
            increment(results, STAT_BATCH_CREATE, System.currentTimeMillis() - ts);
            ts = System.currentTimeMillis();
            try (BufferedWriter writer = resource.getWriter(0l)) {
                for (int i = 0; i < 100; i++) {
                    writer.write(RandomStringUtils.random(1000));
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                resource.close();
                increment(results, STAT_BATCH_WRITE, System.currentTimeMillis() - ts);
            }
            ts = System.currentTimeMillis();
            resource.setState(State.DONE);
            increment(results, STAT_BATCH_RENAME, System.currentTimeMillis() - ts);
        } else {
            throw new RuntimeException("Failed to create staging file");
        }
        ts = System.currentTimeMillis();
        resource = stagingMgr.find(STAGE_PATH, batch.getStagedLocation(), batch.getBatchId());
        if (resource != null) {
            increment(results, STAT_BATCH_FIND, System.currentTimeMillis() - ts);
            ts = System.currentTimeMillis();
            try (BufferedReader reader = resource.getReader()) {
                while (reader.readLine() != null) {
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            } finally {
                resource.close();
                increment(results, STAT_BATCH_READ, System.currentTimeMillis() - ts);
            }
            resource.delete();
        } else {
            throw new RuntimeException("Failed to find staging file");
        }
    }

    protected void increment(Map<String, StagingPerfResult> results, String statName, long millis) {
        StagingPerfResult result = results.get(statName);
        if (result == null) {
            result = new StagingPerfResult(statName);
            results.put(statName, result);
        }
        result.incrementCount(1);
        result.incrementMillis(millis);
    }

    public static List<StagingPerfResult> getEmptyResults() {
        List<StagingPerfResult> list = new ArrayList<StagingPerfResult>();
        list.add(new StagingPerfResult(STAT_LOCK_ACQUIRE));
        list.add(new StagingPerfResult(STAT_BATCH_CREATE));
        list.add(new StagingPerfResult(STAT_BATCH_WRITE));
        list.add(new StagingPerfResult(STAT_BATCH_RENAME));
        list.add(new StagingPerfResult(STAT_BATCH_FIND));
        list.add(new StagingPerfResult(STAT_BATCH_READ));
        return list;
    }

    protected List<StagingPerfResult> getResultsAsList(Map<String, StagingPerfResult> results) {
        List<StagingPerfResult> list = new ArrayList<StagingPerfResult>();
        updateRating(STAT_LOCK_ACQUIRE, results, list, 50, 8000);
        updateRating(STAT_BATCH_CREATE, results, list, 100, 12000);
        updateRating(STAT_BATCH_WRITE, results, list, 5, 200);
        updateRating(STAT_BATCH_RENAME, results, list, 150, 18000);
        updateRating(STAT_BATCH_FIND, results, list, 150, 18000);
        updateRating(STAT_BATCH_READ, results, list, 10, 400);
        return list;
    }

    protected void updateRating(String statName, Map<String, StagingPerfResult> results, List<StagingPerfResult> list,
            long lowCount, long highCount) {
        StagingPerfResult result = results.get(statName);
        if (result != null) {
            long opSec = result.getOperationsPerSecond();
            if (opSec <= lowCount) {
                result.setRating(1.0f);
            } else if (opSec >= highCount) {
                result.setRating(9.9f);
            } else {
                float rating = 9.9f * ((opSec - lowCount) / ((float) (highCount - lowCount)));
                result.setRating(rating);
            }
            list.add(result);
        }
    }
}
