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
package org.jumpmind.symmetric.android;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;

import org.apache.commons.lang3.StringUtils;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.job.IJob;
import org.jumpmind.symmetric.job.IJobManager;
import org.jumpmind.symmetric.model.JobDefinition;
import org.jumpmind.symmetric.service.IParameterService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AndroidJobManager implements IJobManager {
    protected final Logger log = LoggerFactory.getLogger(getClass());
    protected ISymmetricEngine engine;
    protected Job job;
    protected long lastPullTime = System.currentTimeMillis();
    protected long lastPushTime = System.currentTimeMillis();
    protected long lastHeartbeatTime = System.currentTimeMillis();
    protected long lastPurgeTime = System.currentTimeMillis();
    protected long lastRouteTime = System.currentTimeMillis();
    protected long lastFileSyncPullTime = System.currentTimeMillis();
    protected long lastFileSyncTrackerTime = System.currentTimeMillis();
    protected long lastFileSyncPushTime = System.currentTimeMillis();
    protected boolean started = false;

    public AndroidJobManager(ISymmetricEngine engine) {
        this.engine = engine;
    }

    @Override
    public boolean isStarted() {
        return started;
    }

    public List<IJob> getJobs() {
        List<IJob> jobs = new ArrayList<IJob>(1);
        if (job != null) {
            jobs.add(job);
        }
        return jobs;
    }

    public void startJobs() {
        if (job == null) {
            job = new Job();
            job.start();
        }
        started = true;
    }

    public void stopJobs() {
        if (job != null) {
            job.stop();
        }
        started = false;
    }

    public IJob getJob(String name) {
        return job != null ? job : null;
    }

    public void destroy() {
        stopJobs();
    }

    class Job extends TimerTask implements IJob {
        Timer timer;
        Date lastFinishTime;
        boolean running = false;
        boolean cancel = false;
        boolean started = false;
        boolean paused = false;
        long numberOfRuns = 0;
        long totalRunTimeInMs = 0;
        long lastExecutionTimeInMs = 0;

        @Override
        public void run() {
            invoke(false);
        }

        public boolean invoke(boolean force) {
            boolean didWork = false;
            if (!paused && started && engine.isStarted()) {
                long ts = System.currentTimeMillis();
                try {
                    running = true;
                    IParameterService parameterService = engine.getParameterService();
                    String startRouteJob = parameterService.getString(ParameterConstants.START_ROUTE_JOB_38);
                    boolean startRoutingJob = false;
                    if (StringUtils.isBlank(startRouteJob)) {
                        startRoutingJob = parameterService.is(ParameterConstants.START_ROUTE_JOB);
                    } else {
                        startRoutingJob = parameterService.is(ParameterConstants.START_ROUTE_JOB_38);
                    }
                    if (startRoutingJob
                            && parameterService.getInt(ParameterConstants.JOB_ROUTING_PERIOD_TIME_MS) < System.currentTimeMillis() - lastRouteTime) {
                        try {
                            engine.route();
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastRouteTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.START_PUSH_JOB)
                            && parameterService.getInt(ParameterConstants.JOB_PUSH_PERIOD_TIME_MS) < System
                                    .currentTimeMillis() - lastPushTime) {
                        try {
                            didWork = true;
                            engine.push();
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastPushTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.START_PULL_JOB)
                            && parameterService.getInt(ParameterConstants.JOB_PULL_PERIOD_TIME_MS) < System
                                    .currentTimeMillis() - lastPullTime) {
                        try {
                            didWork = true;
                            engine.pull();
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastPullTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.START_HEARTBEAT_JOB)
                            && parameterService.getInt(ParameterConstants.HEARTBEAT_JOB_PERIOD_MS) < System
                                    .currentTimeMillis() - lastHeartbeatTime) {
                        try {
                            didWork = true;
                            engine.heartbeat(false);
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastHeartbeatTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.START_PURGE_INCOMING_JOB)
                            && parameterService.getInt("job.purge.period.time.ms") < System
                                    .currentTimeMillis() - lastPurgeTime) {
                        try {
                            didWork = true;
                            engine.purge();
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastPurgeTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)
                            && parameterService.is(ParameterConstants.START_FILE_SYNC_TRACKER_JOB)
                            && parameterService.getLong("job.file.sync.tracker.period.time.ms", 5000) < (System
                                    .currentTimeMillis() - lastFileSyncTrackerTime)) {
                        try {
                            didWork = true;
                            engine.getFileSyncService().trackChanges(false);
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastFileSyncTrackerTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)
                            && parameterService.is(ParameterConstants.START_FILE_SYNC_PULL_JOB)
                            && parameterService.getLong(ParameterConstants.JOB_FILE_SYNC_PULL_PERIOD_TIME_MS, 60000) < (System
                                    .currentTimeMillis() - lastFileSyncPullTime)) {
                        try {
                            didWork = true;
                            engine.getFileSyncService().pullFilesFromNodes(false);
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastFileSyncPullTime = System.currentTimeMillis();
                        }
                    }
                    if (parameterService.is(ParameterConstants.FILE_SYNC_ENABLE)
                            && parameterService.is(ParameterConstants.START_FILE_SYNC_PUSH_JOB)
                            && parameterService.getLong(ParameterConstants.JOB_FILE_SYNC_PUSH_PERIOD_TIME_MS, 60000) < (System
                                    .currentTimeMillis() - lastFileSyncPushTime)) {
                        try {
                            didWork = true;
                            engine.getFileSyncService().pushFilesToNodes(false);
                        } catch (Throwable ex) {
                            log.error("", ex);
                        } finally {
                            lastFileSyncPushTime = System.currentTimeMillis();
                        }
                    }
                } finally {
                    if (didWork) {
                        numberOfRuns++;
                    }
                    lastExecutionTimeInMs = System.currentTimeMillis() - ts;
                    totalRunTimeInMs += lastExecutionTimeInMs;
                    lastFinishTime = new Date();
                    running = false;
                }
            }
            return true;
        }

        public String getClusterLockName() {
            return getName();
        }

        public synchronized void start() {
            if (!started) {
                if (timer == null) {
                    timer = new Timer();
                }
                timer.scheduleAtFixedRate(this, 1000, 1000);
                started = true;
            }
        }

        public synchronized boolean stop() {
            cancel();
            if (timer != null) {
                timer.cancel();
                timer = null;
            }
            started = false;
            job = null;
            return true;
        }

        public String getName() {
            return "AndroidJob";
        }

        public void pause() {
            paused = true;
        }

        public void unpause() {
            paused = false;
        }

        public boolean isPaused() {
            return paused;
        }

        public boolean isStarted() {
            return started;
        }

        public boolean isRunning() {
            return running;
        }

        public boolean isAutoStartConfigured() {
            return true;
        }

        public long getLastExecutionTimeInMs() {
            return lastExecutionTimeInMs;
        }

        public long getAverageExecutionTimeInMs() {
            return numberOfRuns > 0 ? totalRunTimeInMs / numberOfRuns : 0;
        }

        public Date getLastFinishTime() {
            return lastFinishTime;
        }

        public long getNumberOfRuns() {
            return numberOfRuns;
        }

        public long getTotalExecutionTimeInMs() {
            return totalRunTimeInMs;
        }

        public long getTimeBetweenRunsInMs() {
            return 1000l;
        }

        @Override
        public JobDefinition getJobDefinition() {
            return null;
        }

        @Override
        public Date getNextExecutionTime() {
            return null;
        }

        @Override
        public boolean isCronSchedule() {
            return false;
        }

        @Override
        public boolean isPeriodicSchedule() {
            return false;
        }

        @Override
        public String getSchedule() {
            return null;
        }

        @Override
        public String getDeprecatedStartParameter() {
            return null;
        }
    }

    @Override
    public void restartJobs() {
        this.stopJobs();
        this.startJobs();
    }

    @Override
    public void init() {
        // No action on Android
    }

    @Override
    public void saveJob(JobDefinition jobDefinition) {
        // No action on Android
    }

    @Override
    public void removeJob(String name) {
    }

    @Override
    public boolean isJobApplicableToNodeGroup(IJob job) {
        return false;
    }
}
