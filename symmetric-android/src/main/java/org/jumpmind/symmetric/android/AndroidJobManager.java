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

import org.apache.commons.lang.NotImplementedException;
import org.jumpmind.symmetric.ISymmetricEngine;
import org.jumpmind.symmetric.common.ParameterConstants;
import org.jumpmind.symmetric.job.IJob;
import org.jumpmind.symmetric.job.IJobManager;
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

    public AndroidJobManager(ISymmetricEngine engine) {
        this.engine = engine;
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
    }

    public void stopJobs() {
        if (job != null) {
            job.stop();
        }
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

                    if (parameterService.is(ParameterConstants.START_ROUTE_JOB)
                            && parameterService.getInt("job.routing.period.time.ms") < System
                                    .currentTimeMillis() - lastRouteTime) {
                        try {
                            engine.route();
                        } catch (Throwable ex) {
                            log.error(ex.getMessage(), ex);
                        } finally {
                            lastRouteTime = System.currentTimeMillis();
                        }
                    }

                    if (parameterService.is(ParameterConstants.START_PUSH_JOB)
                            && parameterService.getInt("job.push.period.time.ms") < System
                                    .currentTimeMillis() - lastPushTime) {
                        try {
                            didWork = true;
                            engine.push();
                        } catch (Throwable ex) {
                            log.error(ex.getMessage(), ex);
                        } finally {
                            lastPushTime = System.currentTimeMillis();
                        }
                    }

                    if (parameterService.is(ParameterConstants.START_PULL_JOB)
                            && parameterService.getInt("job.pull.period.time.ms") < System
                                    .currentTimeMillis() - lastPullTime) {
                        try {
                            didWork = true;
                            engine.pull();
                        } catch (Throwable ex) {
                            log.error(ex.getMessage(), ex);
                        } finally {
                            lastPullTime = System.currentTimeMillis();
                        }
                    }

                    if (parameterService.is(ParameterConstants.START_HEARTBEAT_JOB)
                            && parameterService.getInt("job.heartbeat.period.time.ms") < System
                                    .currentTimeMillis() - lastHeartbeatTime) {
                        try {
                            didWork = true;
                            engine.heartbeat(false);
                        } catch (Throwable ex) {
                            log.error(ex.getMessage(), ex);
                        } finally {
                            lastHeartbeatTime = System.currentTimeMillis();
                        }
                    }

                    if (parameterService.is(ParameterConstants.START_PURGE_JOB)
                            && parameterService.getInt("job.purge.period.time.ms") < System
                                    .currentTimeMillis() - lastPurgeTime) {
                        try {
                            didWork = true;
                            engine.purge();
                        } catch (Throwable ex) {
                            log.error(ex.getMessage(), ex);
                        } finally {
                            lastPurgeTime = System.currentTimeMillis();
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

        public String getCronExpression() {
            throw new NotImplementedException();
        }

        public long getTimeBetweenRunsInMs() {
            return 1000l;
        }

        public void setCronExpression(String cronExpression) {
            throw new NotImplementedException();
        }

        public void setTimeBetweenRunsInMs(long timeBetweenRunsInMs) {
            throw new NotImplementedException();
        }
    }

}
